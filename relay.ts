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

// ─── Connexion Spectrum (avec reconnexion) ─────────────────────────────────────

let app: Awaited<ReturnType<typeof Spectrum>>;
// Le SDK spectrum-ts a des types stricts qui n'exposent pas user()/space()/
// getAttachment() tels qu'on les appelle (alors que les méthodes existent bien
// à l'exécution, cf. doc). On type `im` en `any` pour laisser passer le build ;
// on perd la vérif de types sur ces appels, mais ils sont corrects au runtime.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let im: any;

async function connect() {
  app = await Spectrum({
    projectId: process.env.PROJECT_ID!,
    projectSecret: process.env.PROJECT_SECRET!,
    providers: [imessage.config()],
  });
  im = imessage(app);
  console.log("[relay] Spectrum connected");
}

await connect();

// ─── Détection des erreurs de connexion + reconnexion ──────────────────────────

const CONNECTION_ERROR_PATTERNS = [
  "connection dropped",
  "no connection established",
  "failed to connect",
  "connection closed",
  "connection reset",
  "econnreset",
  "socket hang up",
  "stream closed",
  "stream dead",
  "timeout",
  "upstream",
];

function isConnectionError(err: unknown): boolean {
  const msg = (err instanceof Error ? err.message : String(err)).toLowerCase();
  return CONNECTION_ERROR_PATTERNS.some((p) => msg.includes(p));
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Transforme un blocage en erreur. Si `fn` ne résout pas en `ms`, on rejette
 * avec un message contenant "connection" → withReconnect le traite comme une
 * erreur de connexion et reconnecte + retry. Sans ça, un space.send() sur une
 * connexion zombie reste suspendu à l'infini (ni 200, ni 500 : juste le silence).
 */
function withTimeout<T>(fn: () => Promise<T>, ms: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const t = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error(`${label} timeout après ${ms}ms (connection stream dead)`));
    }, ms);
    fn().then(
      (v) => {
        if (settled) return;
        settled = true;
        clearTimeout(t);
        resolve(v);
      },
      (e) => {
        if (settled) return;
        settled = true;
        clearTimeout(t);
        reject(e);
      },
    );
  });
}

/** Timeout par opération Spectrum (envoi/typing). Au-delà = connexion morte. */
const SPECTRUM_OP_TIMEOUT_MS = parseInt(process.env.SPECTRUM_OP_TIMEOUT_MS ?? "8000", 10);

// ─── Déduplication des envois (clientGuid) ─────────────────────────────────────
//
// Lovable retry /send (avec le MÊME clientGuid) quand il n'a pas reçu de
// providerMessageId. Or "200 sans GUID" peut signifier que le message EST parti
// mais que Spectrum n'a pas confirmé d'id. Sans dédup ici, le retry renverrait
// le message une 2e fois → doublon ("type Élodie"). On mémorise donc le
// résultat par clientGuid : un retry avec un clientGuid déjà vu renvoie le
// résultat mémorisé SANS ré-émettre vers Spectrum.

type SendResult = { ok: true; clientGuid: string; id: string };
const SEND_DEDUP_TTL_MS = parseInt(process.env.SEND_DEDUP_TTL_MS ?? "600000", 10); // 10 min
const sendCache = new Map<string, { at: number; result: SendResult }>();
// In-flight : si un 2e appel arrive pendant que le 1er est encore en cours
// d'envoi, il attend le même résultat au lieu de lancer un 2e envoi.
const sendInflight = new Map<string, Promise<SendResult>>();

function cacheCleanup() {
  const now = Date.now();
  for (const [k, v] of sendCache) {
    if (now - v.at > SEND_DEDUP_TTL_MS) sendCache.delete(k);
  }
}

// Dédupe : si plusieurs requêtes échouent en même temps, elles partagent
// la même reconnexion au lieu d'en déclencher une chacune.
let reconnecting: Promise<void> | null = null;

function reconnect(): Promise<void> {
  if (reconnecting) return reconnecting;
  reconnecting = (async () => {
    console.warn("[relay] connexion perdue — reconnexion Spectrum…");
    try {
      await app?.stop();
    } catch {
      /* best-effort : l'ancienne connexion est déjà morte */
    }
    await connect();
  })().finally(() => {
    reconnecting = null;
  });
  return reconnecting;
}

/**
 * Exécute `fn`. Si elle échoue avec une erreur de connexion upstream,
 * on reconnecte Spectrum puis on réessaie UNE fois.
 *
 * Note : sans danger pour /send ici car les drops échouent vite
 * (avant que le message ne parte), donc pas de double envoi.
 */
async function withReconnect<T>(label: string, fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    if (!isConnectionError(err)) throw err;
    console.warn(`[relay] ${label}: ${(err as Error).message} → reconnexion + retry`);
    await reconnect();
    await sleep(300); // laisse la connexion se stabiliser
    return await fn();
  }
}

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

async function handleSend(body: Record<string, string>): Promise<SendResult> {
  const { phone, text, clientGuid } = body;
  console.log("[relay] send attempt:", { phone, textLen: text?.length, clientGuid });

  // Dédup : ce clientGuid a-t-il déjà été envoyé (ou est-il en cours) ?
  if (clientGuid) {
    cacheCleanup();
    const cached = sendCache.get(clientGuid);
    if (cached) {
      console.log("[relay] send dedup HIT (déjà envoyé) :", clientGuid);
      return cached.result;
    }
    const inflight = sendInflight.get(clientGuid);
    if (inflight) {
      console.log("[relay] send dedup WAIT (envoi en cours) :", clientGuid);
      return inflight;
    }
  }

  const doSend = (async (): Promise<SendResult> => {
    const msg = await withReconnect("send", async () => {
      const user = await withTimeout(() => im.user(phone), SPECTRUM_OP_TIMEOUT_MS, "send.user");
      const space: any = await withTimeout(() => im.space(user), SPECTRUM_OP_TIMEOUT_MS, "send.space");
      return await withTimeout(() => space.send(text), SPECTRUM_OP_TIMEOUT_MS, "send.send");
    });

    const realProviderId =
      (msg as any)?.id ??
      (msg as any)?.guid ??
      (msg as any)?.messageId;

    const id = realProviderId ?? (msg as any)?.clientGuid ?? clientGuid ?? crypto.randomUUID();

    const result: SendResult = { ok: true, clientGuid: id, id };

    // On ne mémorise (= on ne bloque les futurs retries) QUE si Spectrum a
    // renvoyé un vrai id. Si pas d'id réel, Lovable repassera en retry : on
    // veut laisser ce retry retenter un vrai envoi, donc on ne cache PAS.
    if (clientGuid && realProviderId) {
      sendCache.set(clientGuid, { at: Date.now(), result });
    }
    return result;
  })();

  if (clientGuid) {
    sendInflight.set(clientGuid, doSend);
    try {
      return await doSend;
    } finally {
      sendInflight.delete(clientGuid);
    }
  }
  return doSend;
}

async function handleMarkRead(body: Record<string, string>) {
  const { phone } = body;
  if (!phone) throw new Error("phone is required");
  return { ok: true };
}

async function handleGetSpace(body: Record<string, string>) {
  const { phone } = body;
  if (!phone) throw new Error("phone is required");

  const space = await withReconnect("get-space", async () => {
    const user = await withTimeout(() => im.user(phone), SPECTRUM_OP_TIMEOUT_MS, "get-space.user");
    return await withTimeout(() => im.space(user), SPECTRUM_OP_TIMEOUT_MS, "get-space.space");
  });

  return {
    ok: true,
    type: (space as any)?.type ?? "dm",
    phone: (space as any)?.phone ?? phone,
  };
}

async function handleTyping(body: Record<string, string>, start: boolean) {
  const { phone } = body;
  if (!phone) throw new Error("phone is required");

  await withReconnect(start ? "typing/start" : "typing/stop", async () => {
    const user = await withTimeout(() => im.user(phone), SPECTRUM_OP_TIMEOUT_MS, "typing.user");
    const space: any = await withTimeout(() => im.space(user), SPECTRUM_OP_TIMEOUT_MS, "typing.space");
    if (start) await withTimeout(() => space.startTyping(), SPECTRUM_OP_TIMEOUT_MS, "typing.start");
    else await withTimeout(() => space.stopTyping(), SPECTRUM_OP_TIMEOUT_MS, "typing.stop");
  });

  return { ok: true };
}

async function handleSendAttachment(body: Record<string, string>) {
  const { phone, base64, mime, filename } = body;
  if (!phone || !base64 || !mime) throw new Error("phone, base64 and mime are required");

  const { attachment } = await import("spectrum-ts");
  const buf = Buffer.from(base64, "base64");

  const msg = await withReconnect("send-attachment", async () => {
    const user = await withTimeout(() => im.user(phone), SPECTRUM_OP_TIMEOUT_MS, "att.user");
    const space: any = await withTimeout(() => im.space(user), SPECTRUM_OP_TIMEOUT_MS, "att.space");
    return await withTimeout(() => space.send(attachment(buf, { name: filename, mimeType: mime })), SPECTRUM_OP_TIMEOUT_MS, "att.send");
  });

  const id =
    (msg as any)?.id ??
    (msg as any)?.clientGuid ??
    (msg as any)?.guid ??
    crypto.randomUUID();

  return { ok: true, id, clientGuid: id };
}

async function handleGetAttachment(body: Record<string, string>) {
  const guid = body.guid ?? body.id; // Lovable envoie `id`, on accepte les deux
  const { phone } = body;
  if (!guid) throw new Error("guid/id is required");

  console.log("[relay] get-attachment attempt:", { guid, phone });

  // Il n'y a PAS d'endpoint REST public pour DL un attachment : Spectrum passe
  // par le SDK (gRPC/WS propriétaire). On utilise donc im.getAttachment(guid,
  // phone) → .read(), via la même connexion que le reste, avec timeout + retry.
  const att = await withReconnect("get-attachment", async () =>
    withTimeout(() => im.getAttachment(guid, phone), SPECTRUM_OP_TIMEOUT_MS, "get-attachment"),
  );

  if (!att) {
    // Attachment introuvable côté Spectrum (guid inconnu / expiré).
    const err = new Error("attachment not found") as Error & { statusCode?: number };
    err.statusCode = 404;
    throw err;
  }

  const bytes: Buffer = Buffer.from(
    await withTimeout(() => (att as any).read(), SPECTRUM_OP_TIMEOUT_MS, "get-attachment.read"),
  );
  const mimeType = (att as any).mimeType ?? "application/octet-stream";
  const name = (att as any).fileName ?? "attachment";

  return { ok: true, base64: bytes.toString("base64"), mimeType, name };
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
  "POST /get-attachment":  (b) => handleGetAttachment(b),
};

const server = createServer(async (req, res) => {
  if (req.url !== "/health" && !isAuthorized(req)) {
    return json(res, 401, { error: "unauthorized" });
  }

  const url2 = new URL(req.url!, `http://localhost`);
  const routeKey = `${req.method} ${url2.pathname}`;
  const handler = ROUTES[routeKey];

  if (!handler) {
    return json(res, 404, { error: "not found", route: routeKey });
  }

  try {
    const body = (req.method === "GET"
      ? Object.fromEntries(url2.searchParams.entries())
      : await readBody(req)) as Record<string, string>;
    const result = await handler(body);
    json(res, 200, result);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    const statusCode = (err as { statusCode?: number })?.statusCode ?? 500;
    console.error(`[relay] ${routeKey} failed:`, message);
    json(res, statusCode, { error: message });
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
