// UUIDv7 (RFC 9562): time-ordered, sortable ids. Used for messageId and any
// entity id you want to sort chronologically. Monotonic within the same ms.
let lastTs = 0;
let seq = 0;

export function uuidv7(): string {
  let ts = Date.now();
  if (ts < lastTs) ts = lastTs; // never go backwards on clock skew
  if (ts === lastTs) seq = (seq + 1) & 0x0fff;
  else { lastTs = ts; seq = 0; }

  const b = new Uint8Array(16);
  b[0] = Math.floor(ts / 2 ** 40) & 0xff;
  b[1] = Math.floor(ts / 2 ** 32) & 0xff;
  b[2] = Math.floor(ts / 2 ** 24) & 0xff;
  b[3] = Math.floor(ts / 2 ** 16) & 0xff;
  b[4] = Math.floor(ts / 2 ** 8) & 0xff;
  b[5] = ts & 0xff;

  const rnd = new Uint8Array(8);
  globalThis.crypto.getRandomValues(rnd);
  b[6] = 0x70 | ((seq >> 8) & 0x0f); // version 7 + high nibble of seq
  b[7] = seq & 0xff;
  b[8] = 0x80 | (rnd[0] & 0x3f); // variant
  for (let i = 9; i < 16; i++) b[i] = rnd[i - 8];

  const h = [...b].map((x) => x.toString(16).padStart(2, "0"));
  return `${h[0]}${h[1]}${h[2]}${h[3]}-${h[4]}${h[5]}-${h[6]}${h[7]}-${h[8]}${h[9]}-${h[10]}${h[11]}${h[12]}${h[13]}${h[14]}${h[15]}`;
}
