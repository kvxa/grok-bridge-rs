const WEB_UI_HEADER = { "X-Grok-Bridge-WebUI": "1" };

async function responseError(response) {
  const message = await response.text();
  return message || `${response.status} ${response.statusText}`;
}

export async function getSessions() {
  const response = await fetch("/api/sessions", { cache: "no-store" });
  if (!response.ok) throw new Error(await responseError(response));
  return response.json();
}

export async function closeSessionRequest(id) {
  const response = await fetch(
    `/api/sessions/${encodeURIComponent(id)}/close`,
    { method: "POST", headers: WEB_UI_HEADER },
  );
  if (!response.ok) throw new Error(await responseError(response));
}

export async function closeOwnerRequest(owner) {
  const response = await fetch(
    `/api/owners/${encodeURIComponent(owner)}/close`,
    { method: "POST", headers: WEB_UI_HEADER },
  );
  if (!response.ok) throw new Error(await responseError(response));
  return response.json();
}
