use alloy::primitives::Address;
use alloy::network::{EthereumWallet, Ethereum};
use alloy::providers::{ProviderBuilder, DynProvider};
use alloy::rpc::client::RpcClient;
use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket, ResponsePayload};
use alloy::transports::http::Http;
use alloy::transports::layers::FallbackLayer;
use alloy::transports::{TransportError, TransportErrorKind, TransportFut};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::task::{Context, Poll};
use std::time::Duration;
use std::collections::HashMap;
use tower::{Service, ServiceBuilder};
use tracing::warn;

const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Returns `true` for JSON-RPC error responses that indicate the endpoint itself is
/// broken (misconfigured, unauthorized, rate-limited, overloaded) rather than an
/// application-level error (e.g. a contract revert).
///
/// alloy's `FallbackLayer` only fails over on transport-level errors (connection
/// refused, timeout, HTTP 5xx); a JSON-RPC error delivered over a valid HTTP 200
/// response is otherwise treated as a "successful" call, so a misconfigured endpoint
/// that responds quickly with an auth error can win the fallback race indefinitely
/// and never gets scored down. `ErrorAwareTransport` below reclassifies these specific
/// cases as transport errors so the fallback logic actually routes around them.
fn is_infra_rpc_error(code: i64, message: &str) -> bool {
    // -32000 ("server error") is deliberately excluded from this code-only match: it's a
    // broad, provider-defined catch-all that some nodes also use for application-level
    // reverts (with a `data` payload we must not discard). Its auth/rate-limit cases are
    // still caught below via message text, which is unambiguous.
    matches!(code, -32603 | -32005)
        || message.contains("Unauthorized")
        || message.contains("API key")
        || message.contains("api key")
        || message.contains("rate limit")
        || message.contains("Too Many Requests")
}

/// Wraps an RPC transport so JSON-RPC responses matching [`is_infra_rpc_error`] are
/// surfaced as transport errors instead of being passed through as a successful call.
#[derive(Clone)]
struct ErrorAwareTransport<S> {
    inner: S,
    label: String,
}

impl<S> Service<RequestPacket> for ErrorAwareTransport<S>
where
    S: Service<RequestPacket, Response = ResponsePacket, Error = TransportError, Future = TransportFut<'static>>
        + Send
        + Clone
        + 'static,
{
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        let label = self.label.clone();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let response = fut.await?;

            let infra_error = match &response {
                ResponsePacket::Single(r) => match &r.payload {
                    ResponsePayload::Failure(e) if is_infra_rpc_error(e.code, &e.message) => {
                        Some(format!("error code {}: {}", e.code, e.message))
                    }
                    _ => None,
                },
                ResponsePacket::Batch(rs) => rs.iter().find_map(|r| match &r.payload {
                    ResponsePayload::Failure(e) if is_infra_rpc_error(e.code, &e.message) => {
                        Some(format!("error code {}: {}", e.code, e.message))
                    }
                    _ => None,
                }),
            };

            if let Some(msg) = infra_error {
                warn!(logger = "Config", transport = label.as_str(), "RPC endpoint returned infra-level error, treating as unavailable for fallback purposes: {msg}");
                return Err(TransportErrorKind::custom_str(&msg));
            }

            Ok(response)
        })
    }
}

#[derive(Debug, Clone)]
pub struct ChainInfo {
    pub name: String,
    pub rpc_urls: Vec<String>,
    pub deposit_token: Option<Address>,
    pub avg_block_millis: u32,
}

#[derive(Clone)]
pub struct RouteSettings {
    pub relay_delay_secs: u64,
    pub start_verification_delay: u64,
    pub min_challenge_period: u64,
    pub sync_lookback_secs: u64,
}

impl RouteSettings {
    pub fn test_defaults() -> Self {
        Self {
            relay_delay_secs: 7 * 24 * 3600,
            start_verification_delay: 86400 + 3600,
            min_challenge_period: 600,
            sync_lookback_secs: 7 * 24 * 3600 + 24 * 3600,
        }
    }
}

#[derive(Clone)]
pub struct Route {
    pub name: &'static str,
    pub inbox_chain_id: u64,
    pub inbox_address: Address,
    pub inbox_provider: DynProvider<Ethereum>,
    pub inbox_avg_block_millis: u32,
    pub outbox_chain_id: u64,
    pub outbox_address: Address,
    pub outbox_provider: DynProvider<Ethereum>,
    pub weth_address: Option<Address>,
    pub settings: RouteSettings,
}

#[derive(Clone)]
pub struct ValidatorConfig {
    pub private_key: String,
    pub wallet: EthereumWallet,
    pub chains: HashMap<u64, ChainInfo>,
    pub inbox_arb_to_eth: Address,
    pub outbox_arb_to_eth: Address,
    pub inbox_arb_to_gnosis: Address,
    pub outbox_arb_to_gnosis: Address,
    pub arb_outbox: Address,
    pub sequencer_inbox: Option<Address>,
    pub ethereum_provider: DynProvider<Ethereum>,
    pub make_claims: bool,
}
/// Strips path/query from an RPC URL so it's safe to log (RPC URLs commonly embed
/// API keys in the path, e.g. `https://rpc.ankr.com/eth_sepolia/<key>`).
fn redact_url(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => {
            // Secrets can appear in the path (`/eth_sepolia/<key>`), the query string
            // (`?apikey=<key>`), or userinfo (`user:pass@host`) - strip all three.
            let host_and_after = rest.split_once('@').map_or(rest, |(_, after)| after);
            let host = host_and_after
                .split(['/', '?', '#'])
                .next()
                .unwrap_or(host_and_after);
            format!("{scheme}://{host}")
        }
        None => "unknown".to_string(),
    }
}

fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(RPC_TIMEOUT)
        .build()
        .expect("Failed to build HTTP client")
}

fn build_provider_from_urls(urls: &[String], wallet: &EthereumWallet) -> DynProvider<Ethereum> {
    let http_client = build_http_client();

    if urls.len() == 1 {
        let transport = Http::with_client(http_client, urls[0].parse().expect("Invalid RPC URL"));
        let client = RpcClient::builder().transport(transport, false);
        return DynProvider::new(
            ProviderBuilder::new()
                .wallet(wallet.clone())
                .connect_client(client)
        );
    }

    let fallback = FallbackLayer::default()
        .with_active_transport_count(NonZeroUsize::new(2).unwrap());

    let transports: Vec<_> = urls.iter()
        .map(|url| ErrorAwareTransport {
            inner: Http::with_client(http_client.clone(), url.parse().expect("Invalid RPC URL")),
            label: redact_url(url),
        })
        .collect();

    let transport = ServiceBuilder::new()
        .layer(fallback)
        .service(transports);

    let client = RpcClient::builder().transport(transport, false);
    DynProvider::new(
        ProviderBuilder::new()
            .wallet(wallet.clone())
            .connect_client(client)
    )
}

impl ValidatorConfig {
    fn build_provider(&self, chain_id: u64) -> DynProvider<Ethereum> {
        let chain = self.chains.get(&chain_id).expect("Chain not found");
        build_provider_from_urls(&chain.rpc_urls, &self.wallet)
    }

    pub fn build_routes(&self) -> Vec<Route> {
        let arb_provider = self.build_provider(42161);
        let eth_provider = self.build_provider(1);
        let gnosis_provider = self.build_provider(100);

        vec![
            Route {
                name: "ARB_TO_ETH",
                inbox_chain_id: 42161,
                inbox_address: self.inbox_arb_to_eth,
                inbox_provider: arb_provider.clone(),
                inbox_avg_block_millis: 250,
                outbox_chain_id: 1,
                outbox_address: self.outbox_arb_to_eth,
                outbox_provider: eth_provider.clone(),
                weth_address: self.chains.get(&1).expect("Ethereum").deposit_token,
                settings: RouteSettings::test_defaults(),
            },
            Route {
                name: "ARB_TO_GNOSIS",
                inbox_chain_id: 42161,
                inbox_address: self.inbox_arb_to_gnosis,
                inbox_provider: arb_provider.clone(),
                inbox_avg_block_millis: 250,
                outbox_chain_id: 100,
                outbox_address: self.outbox_arb_to_gnosis,
                outbox_provider: gnosis_provider.clone(),
                weth_address: self.chains.get(&100).expect("Gnosis").deposit_token,
                settings: RouteSettings::test_defaults(),
            },
        ]
    }

    fn parse_rpc_urls(env_var: &str) -> Vec<String> {
        std::env::var(env_var)
            .expect(&format!("{} must be set", env_var))
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {

        let arbitrum_rpcs = Self::parse_rpc_urls("ARBITRUM_RPC_URL");
        let ethereum_rpcs = Self::parse_rpc_urls("ETHEREUM_RPC_URL");
        let gnosis_rpcs = Self::parse_rpc_urls("GNOSIS_RPC_URL");
        let weth_gnosis = Address::from_str(
            &std::env::var("WETH_GNOSIS")
                .expect("WETH_GNOSIS must be set")
        )?;

        let private_key = std::env::var("PRIVATE_KEY")
            .or_else(|_| std::fs::read_to_string("/run/secrets/validator_key")
                .map(|s| s.trim().to_string()))
            .expect("PRIVATE_KEY not set or /run/secrets/validator_key not found");

        use alloy::signers::local::PrivateKeySigner;
        let signer = PrivateKeySigner::from_str(&private_key)?;
        let wallet = EthereumWallet::from(signer);

        let mut chains = HashMap::new();
        chains.insert(42161, ChainInfo {
            name: "Arbitrum".to_string(),
            rpc_urls: arbitrum_rpcs,
            deposit_token: None,
            avg_block_millis: 250,
        });
        chains.insert(1, ChainInfo {
            name: "Ethereum".to_string(),
            rpc_urls: ethereum_rpcs,
            deposit_token: None,
            avg_block_millis: 12000,
        });
        chains.insert(100, ChainInfo {
            name: "Gnosis".to_string(),
            rpc_urls: gnosis_rpcs,
            deposit_token: Some(weth_gnosis),
            avg_block_millis: 5000,
        });

        let inbox_arb_to_eth = Address::from_str(
            &std::env::var("VEA_INBOX_ARB_TO_ETH")
                .expect("VEA_INBOX_ARB_TO_ETH must be set")
        )?;
        let outbox_arb_to_eth = Address::from_str(
            &std::env::var("VEA_OUTBOX_ARB_TO_ETH")
                .expect("VEA_OUTBOX_ARB_TO_ETH must be set")
        )?;
        let inbox_arb_to_gnosis = Address::from_str(
            &std::env::var("VEA_INBOX_ARB_TO_GNOSIS")
                .expect("VEA_INBOX_ARB_TO_GNOSIS must be set")
        )?;
        let outbox_arb_to_gnosis = Address::from_str(
            &std::env::var("VEA_OUTBOX_ARB_TO_GNOSIS")
                .expect("VEA_OUTBOX_ARB_TO_GNOSIS must be set")
        )?;
        let arb_outbox = Address::from_str(
            &std::env::var("ARB_OUTBOX")
                .expect("ARB_OUTBOX must be set")
        )?;

        let make_claims = std::env::var("MAKE_CLAIMS")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .expect("MAKE_CLAIMS must be set");

        let sequencer_inbox = std::env::var("SEQUENCER_INBOX")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| Address::from_str(&s).expect("Invalid SEQUENCER_INBOX address"));

        let ethereum_provider = build_provider_from_urls(
            &chains.get(&1).expect("Ethereum chain not configured").rpc_urls,
            &wallet,
        );

        Ok(Self {
            private_key,
            wallet,
            chains,
            inbox_arb_to_eth,
            outbox_arb_to_eth,
            inbox_arb_to_gnosis,
            outbox_arb_to_gnosis,
            arb_outbox,
            sequencer_inbox,
            ethereum_provider,
            make_claims,
        })
    }
}
