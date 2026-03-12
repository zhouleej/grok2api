import type { GrokSettings } from "../settings";

export interface NsfwResult {
  ok: boolean;
  statusCode: number | null;
  grpcStatus: string | null;
  error: string | null;
}

const NSFW_PAYLOAD = new Uint8Array([
  0x00, 0x00, 0x00, 0x00, 0x20,
  0x0a, 0x02, 0x10, 0x01,
  0x12, 0x1a,
  0x0a, 0x18,
  0x61, 0x6c, 0x77, 0x61, 0x79, 0x73, 0x5f, 0x73,
  0x68, 0x6f, 0x77, 0x5f, 0x6e, 0x73, 0x66, 0x77,
  0x5f, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74,
]);

const DEFAULT_TIMEOUT_S = 15;
const GRPC_BASE = "https://grok.com";
const GRPC_PATH = "/auth_mgmt.AuthManagement/UpdateUserFeatureControls";

export async function enableNsfw(
  token: string,
  settings: GrokSettings,
  cfClearance?: string,
): Promise<NsfwResult> {
  if (!token) {
    return { ok: false, statusCode: null, grpcStatus: null, error: "missing token" };
  }

  const baseUrl = (settings.proxy_url ?? "").trim() || GRPC_BASE;
  const url = `${baseUrl.replace(/\/+$/, "")}${GRPC_PATH}`;

  let cookieStr = `sso=${token};sso-rw=${token}`;
  if (cfClearance) {
    cookieStr += `;cf_clearance=${cfClearance}`;
  }

  const headers: Record<string, string> = {
    "content-type": "application/grpc-web+proto",
    "origin": "https://grok.com",
    "referer": "https://grok.com/?_s=data",
    "x-grpc-web": "1",
    "user-agent":
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "cookie": cookieStr,
  };

  const timeoutMs = DEFAULT_TIMEOUT_S * 1000;

  try {
    const response = await fetch(url, {
      method: "POST",
      headers,
      body: NSFW_PAYLOAD,
      signal: AbortSignal.timeout(timeoutMs),
    });

    const grpcStatus = response.headers.get("grpc-status");
    const ok = response.status === 200 && (grpcStatus === null || grpcStatus === "0");

    let error: string | null = null;
    if (response.status === 403) {
      error = "403 Forbidden";
    } else if (response.status !== 200) {
      error = `HTTP ${response.status}`;
    } else if (grpcStatus !== null && grpcStatus !== "0") {
      error = `gRPC ${grpcStatus}`;
    }

    return { ok, statusCode: response.status, grpcStatus, error };
  } catch (e) {
    return { ok: false, statusCode: null, grpcStatus: null, error: String(e) };
  }
}
