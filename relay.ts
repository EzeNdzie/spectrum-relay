/**
 * Spectrum Relay — Node.js long-lived process
 *
 * Tient la connexion gRPC Spectrum et expose une API HTTP simple
 * pour les Cloudflare Workers (stateless) qui ne peuvent pas faire gRPC.
 *
 * Deploy sur Railway / Fly.io / VPS avec Node.js 18+.
 * Variables d'environnement requises :
 *   PROJECT_ID          — Spectrum project ID
 *   PROJECT_SECRET      — Spectrum project secret
 *   RELAY_SECRET        — Secret partagé avec les Workers CF (Bearer token)
 *   PORT                — Port d'écoute (défaut: 3000)
 */

import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { Spectrum } from "spectrum-ts";
import { imessage } from "spectrum-ts/providers/imessage";

// ─── Init Spectrum ────────────────────────────────────────────────────────────

const app = await Spectrum({
  projectId: process.env.PROJECT_ID!,
  projectSecret: process.env.PROJECT_SECRET!,
  providers: [imessage.config()],
});

const im = imessage(app);

console.log("[relay] Spectrum connected");

// ─── Auth middleware ──────────────────────────────────────────────────────────

function isAuthorized(req: IncomingMessage): boolean {
  const auth = req.headers["authorization"];
  return auth === `Bearer ${process.env.RELAY_SECRET}`;
}

// ─── Body parser ─────────────────────────────────────────────────────────────

function readBody(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (chunk) => (data += chunk));
    req.on("end", () => {
      try {
        resolve(JSON.parse(data));
      } catch {
        reject(new Error("Invalid JSON"));
      }
    });
    req.on("error", reject);
  });
}

// ─── Response helpers ─────────────────────────────────────────────────────────

function json(res: ServerResponse, status: number, body: unknown) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": Buffer.byteLength(payload),
  });
  res.end(payload);
}

// ─── Routes ──────────────────────────────────────────────────────────────────

async function handleSend(body: Record<string, string>) {
  const { phone, text, clientGuid, respondingTo } = body;
  console.log('[relay] send attempt:', { phone, textLen: text?.length });

  const user = await im.user(phone);
  const space = await im.space(user);

  if (respondingTo) {
    await space.send(text);
  } else {
    await space.send(text);
  }

  return { ok: true, clientGuid: clientGuid ?? null };
}

async function handleMarkRead(body: Record<string, string>) {
  const { phone } = body;
  if (!phone) throw new Error("phone is required");
  return { ok: true };
}

async function handleGetSpace(body: Record<string, string>) {
  const { phone } = body;
  if (!phone) throw new Error("phone is required");

  const user = await im.user(phone);
  const space = await im.space(user);

  return {
    ok: true,
    type: (space as any)?.type ?? "dm",
    phone: (space as any)?.phone ?? phone,
  };
}

async function handleTyping(body: Record<string, string>, start: boolean) {
  const { phone } = body;
  if (!phone) throw new Error("phone is required");

  const user = await im.user(phone);
  const space = await im.space(user);

  if (start) await space.startTyping();
  else await space.stopTyping();

  return { ok: true };
}

async function handleSendAttachment(body: Record<string, string>) {
  const { phone, base64, mime, filename } = body;
  if (!phone || !base64 || !mime) throw new Error("phone, base64 and mime are required");

  const { attachment } = await import("spectrum-ts");
  const buf = Buffer.from(base64, "base64");
  const user = await im.user(phone);
  const space = await im.space(user);
  const msg = await space.send(attachment(buf, { name: filename, mimeType: mime }));

  return { ok: true, id: (msg as any)?.id, clientGuid: (msg as any)?.clientGuid };
}

async function handleGetAttachment(body: Record<string, string>) {
  const { guid, phone } = body;
  if (!guid || !phone) throw new Error("guid and phone are required");

  console.log("[relay] get-attachment attempt:", { guid, phone });

  const projectId = process.env.PROJECT_ID!;
  const projectSecret = process.env.PROJECT_SECRET!;

  const resp = await fetch(
    `https://spectrum.photon.codes/projects/${projectId}/attachments/${guid}`,
    {
      headers: {
        Authorization: `Basic ${Buffer.from(`${projectId}:${projectSecret}`).toString("base64")}`,
      },
    }
  );

  if (!resp.ok) throw new Error(`Spectrum attachment fetch failed: ${resp.status}`);

  const mimeType = resp.headers.get("content-type") ?? "image/jpeg";
  const buffer = Buffer.from(await resp.arrayBuffer());

  return { ok: true, base64: buffer.toString("base64"), mimeType };
}

// ─── HTTP Server ─────────────────────────────────────────────────────────────

const ROUTES: Record<string, (body: Record<string, string>) => Promise<unknown>> = {
  "GET /health":           async () => ({ ok: true }),
  "POST /send":            (b) => handleSend(b),
  "POST /mark-read":       (b) => handleMarkRead(b),
  "POST /get-space":       (b) => handleGetSpace(b),
  "POST /typing/start":    (b) => handleTyping(b, true),
  "POST /typing/stop":     (b) => handleTyping(b, false),
  "POST /send-attachment": (b) => handleSendAttachment(b),
  "GET /get-attachment":   (b) => handleGetAttachment(b),
};

const server = createServer(async (req, res) => {
  if (req.url !== '/health' && !isAuthorized(req)) {
    return json(res, 401, { error: "unauthorized" });
  }

  const url2 = new URL(req.url!, `http://localhost`);
  const routeKey = `${req.method} ${url2.pathname}`;
  const handler = ROUTES[routeKey];

  if (!handler) {
    return json(res, 404, { error: "not found", route: routeKey });
  }

  try {
    const body = (req.method === 'GET'
      ? Object.fromEntries(url2.searchParams.entries())
      : await readBody(req)) as Record<string, string>;
    const result = await handler(body);
    json(res, 200, result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[relay] ${routeKey} failed:`, message);
    json(res, 500, { error: message });
  }
});

const PORT = parseInt(process.env.PORT ?? "3000", 10);
server.listen(PORT, () => {
  console.log(`[relay] Listening on :${PORT}`);
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("[relay] SIGTERM — stopping Spectrum");
  await app.stop();
  server.close(() => process.exit(0));
});