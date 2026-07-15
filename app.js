// ── Cached DOM elements ──────────────────────────────────
const asksEl       = document.getElementById("asks");
const bidsEl       = document.getElementById("bids");
const statusEl     = document.getElementById("status");
const tradesEl     = document.getElementById("trades");
const currentPrice = document.getElementById("currentPrice");
const priceChange  = document.getElementById("priceChange");
const priceHigh    = document.getElementById("priceHigh");
const priceLow     = document.getElementById("priceLow");
const imbalanceFill= document.getElementById("imbalanceFill");
const imbalancePct = document.getElementById("imbalancePct");
const whaleRow     = document.getElementById("whaleRow");
const whaleSide    = document.getElementById("whaleSide");
const whaleTime    = document.getElementById("whaleTime");
const whalePrice   = document.getElementById("whalePrice");
const whaleAmount  = document.getElementById("whaleAmount");
const whaleTotal   = document.getElementById("whaleTotal");
const whaleOverlay = document.getElementById("whaleOverlay");

// ── Chart data buffer (rolling window) ────────────────────
let CHART_WINDOW_S = 300;           // default: keep last 5 minutes
document.getElementById("time-window").addEventListener("change", (e) => {
    const newWindow = Number(e.target.value);
    evictOldData(newWindow * 1000);
    CHART_WINDOW_S = newWindow;
    drawChart();
});

function evictOldData(windowMs) {
    const cutoff = Date.now() - windowMs;
    while (historyA.length && historyA[0].t < cutoff) historyA.shift();
    while (historyB.length && historyB[0].t < cutoff) historyB.shift();
}
const SAMPLE_INTERVAL_MS = 1_000;   // new sample every second
const chartCanvas = document.getElementById("chart");
const ctx = chartCanvas.getContext("2d");

let lastSampleTime = 0;
const historyA = [];  // { t, price } – weighted avg ask
const historyB = []; // { t, price } – weighted avg bid

// ── Ticker state ─────────────────────────────────────────
let tickerState = { lastPrice: 0, high24h: 0, low24h: 0, pctChange: 0 };

// ── Recent trades buffer ─────────────────────────────────
const MAX_TRADES   = 30;
let tradeId        = 0;

// ── Depth stream (order book) ────────────────────────────
const depthSocket = new WebSocket(
    "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms"
);

depthSocket.onopen = () => {
    statusEl.className = "status connected";
    statusEl.textContent = "Live";
};

depthSocket.onerror = (error) => {
    console.error("Depth socket error:", error);
};

depthSocket.onmessage = (event) => {
    const data = JSON.parse(event.data);

    renderSide(data.asks, asksEl, "ask");
    renderSide(data.bids, bidsEl, "bid");
    updateImbalance(data.asks, data.bids);

    // Accumulate per-second samples for the chart
    const now = Date.now();
    if (now - lastSampleTime >= SAMPLE_INTERVAL_MS) {
        pushSample(data.asks, data.bids);
        lastSampleTime = now;
    }
};

// ── Ticker + Trade stream ────────────────────────────────
const secondarySocket = new WebSocket(
    "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/btcusdt@trade"
);

secondarySocket.onopen = () => {
    statusEl.className = "status connected";
    statusEl.textContent = "Live";
};

secondarySocket.onclose = (event) => {
    // Only update if depth socket is also down
    if (depthSocket.readyState !== WebSocket.OPEN) {
        statusEl.className = "status disconnected";
        statusEl.textContent = "Disconnected";
    }
};

secondarySocket.onerror = (error) => {
    console.error("Secondary socket error:", error);
};

secondarySocket.onmessage = (event) => {
    const wrapper = JSON.parse(event.data);
    const stream = wrapper.stream;
    const data = wrapper.data;
    if (!data) return;  // defensive: skip malformed messages

    if (stream === "btcusdt@ticker") {
        updateTicker(data);
    }

    if (stream === "btcusdt@trade") {
        addTrade(data);
    }
};

// ── Feature 1: Ticker / Current Price Info Box ───────────
function updateTicker(ticker) {
    // Binance @ticker uses short field names: c=price, h=high, l=low, P=pct change
    const current = +ticker.c;
    const high    = +ticker.h;
    const low     = +ticker.l;
    const changePct = +ticker.P;  // P is already the % (string like "1.730")

    if (isNaN(current)) return;

    tickerState.lastPrice = current;
    tickerState.high24h   = high;
    tickerState.low24h    = low;
    tickerState.pctChange = changePct;

    // Direction: green if current >= prev, red otherwise
    const dir = tickerState._prev !== undefined ? (current >= tickerState._prev) : true;
    tickerState._prev = current;

    currentPrice.textContent  = "$" + formatPrice(current);
    currentPrice.className    = "price-current " + (dir ? "up" : "down");
    priceChange.style.color   = dir ? "#2ed573" : "#ff6b6b";

    const sign = changePct >= 0 ? "+" : "";
    priceChange.textContent = sign + changePct.toFixed(2) + "%";

    if (high > 0) priceHigh.textContent = "H: $" + formatPrice(high);
    if (low > 0)  priceLow.textContent  = "L: $" + formatPrice(low);
}

// ── Feature 2: Recent Trades Feed ────────────────────────
const WHALE_THRESHOLD = 0.5;   // BTC volume to flag as "whale"

function addTrade(trade) {
    const tId = trade.t;
    if (tradeId !== 0 && tId <= tradeId) return;   // dedup / out-of-order
    tradeId = tId;

    const isBuy = !trade.m;                         // m=true means maker sell (filling ask)
    const trEl = document.createElement("tr");
    trEl.className = isBuy ? "bid" : "ask";

    const d = new Date(trade.T);                   // T = trade timestamp (ms)
    const h = d.getHours().toString().padStart(2, "0");
    const m = d.getMinutes().toString().padStart(2, "0");
    const s = d.getSeconds().toString().padStart(2, "0");
    const ms = d.getMilliseconds().toString().padStart(3, "0");

    trEl.innerHTML = `
        <td class="time-col">${h}:${m}:${s}.${ms}</td>
        <td class="price-cell"><span>${formatPrice(+trade.p)}</span></td>
        <td class="amount-col whale-qty" style="font-weight:600">${Number(trade.q).toFixed(5)}</td>
        <td class="total-cell">$${formatTradeTotal(+trade.p, +trade.q)}</td>
    `;

    // Whale trade: pop out of table into an overlay that stays put for 10s
    if (+trade.q >= WHALE_THRESHOLD) {
        showWhale(trade);
    }

    // Insert newest at top
    if (tradesEl.firstChild) {
        tradesEl.insertBefore(trEl, tradesEl.firstChild);
    } else {
        tradesEl.appendChild(trEl);
    }

    // Keep only MAX_TRADES rows
    while (tradesEl.children.length > MAX_TRADES) {
        tradesEl.removeChild(tradesEl.lastChild);
    }
}

/** Show a whale trade in the overlay, hold ~10s then fade out. */
function showWhale(trade) {
    const isBuy = !trade.m;

    whaleSide.textContent     = isBuy ? "BUY" : "SELL";
    whaleSide.style.color     = "#2ed573";
    whaleSide.style.background = "rgba(46,213,115,0.15)";

    const d   = new Date(trade.T);
    whaleTime.textContent      = d.toUTCString().slice(-12, -4);
    whalePrice.textContent     = formatPrice(+trade.p);
    whaleAmount.textContent    = Number(trade.q).toFixed(5) + " BTC";
    whaleTotal.textContent     = "$" + formatTradeTotal(+trade.p, +trade.q);

    // Force reflow so fade-in animation restarts on every new whale
    whaleRow.classList.remove("fade-in", "fading-out");
    void whaleRow.offsetWidth;
    whaleRow.classList.add("fade-in");

    whaleOverlay.style.opacity = "1";

    // 8s in: start fading out (3s fade + 2s buffer)
    setTimeout(() => {
        whaleRow.classList.remove("fade-in");
        whaleRow.classList.add("fading-out");
    }, 8_000);

    // 9.5s: hide overlay container
    setTimeout(() => {
        whaleOverlay.style.opacity = "0";
    }, 9_500);
}

// ── Feature 3: Order Book Imbalance ──────────────────────
function updateImbalance(asks, bids) {
    let sellVol = 0, buyVol = 0;
    for (const [p, q] of asks) sellVol += +p * +q;
    for (const [p, q] of bids) buyVol += +p * +q;

    const total = sellVol + buyVol;
    if (total === 0) return;

    const buyPct = (buyVol / total) * 100;

    // Track buy share: 0% = far LEFT/Sell, 100% = far RIGHT/Buy
    const trackWidth = imbalanceFill.parentElement.clientWidth || 200;
    imbalanceFill.style.left = ((buyPct / 100) * (trackWidth - 10)) + "px";

    // Color: green when buy-dominant → red when sell-dominant
    if (buyPct >= 60) {
        imbalanceFill.style.background = "#2ed573";
        imbalancePct.style.color = "#2ed573";
    } else if (buyPct <= 40) {
        imbalanceFill.style.background = "#ff6b6b";
        imbalancePct.style.color = "#ff6b6b";
    } else {
        imbalanceFill.style.background = "#ffa502";
        imbalancePct.style.color = "#ffa502";
    }

    imbalancePct.textContent = buyPct.toFixed(1) + "%";
}

/** Compute weighted-average price from levels and store in history. */
function pushSample(asks, bids) {
    const askAvg = weightedAvg(asks);
    const bidAvg = weightedAvg(bids);
    const t = Date.now();

    if (askAvg > 0 && !historyA.length) initChartScale([askAvg], [bidAvg]);

    historyA.push({ t, price: askAvg });
    historyB.push({ t, price: bidAvg });

    evictOldData(CHART_WINDOW_S * 1000);

    drawChart();
}

/** quantity-weighted average price from a levels array. */
function weightedAvg(levels) {
    let weightSum = 0, pSum = 0;
    for (const [pStr, qStr] of levels) {
        const p = +pStr, q = +qStr;
        pSum += p * q;
        weightSum += q;
    }
    return weightSum > 0 ? pSum / weightSum : 0;
}

/* ── Chart rendering ────────────────────────────────────── */
const PAD = { top: 14, right: 24, bottom: 28, left: 72 };
const COLORS = {
    ask:   "#ff6b6b",
    bid:   "#2ed573",
    grid:  "rgba(255,255,255,0.045)",
    axis:  "rgba(255,255,255,0.18)",
    label: "#6b7080",
    bg:    "#111927",
};

let chartMin = 0, chartMax = 1;

function initChartScale(aPrices, bPrices) {
    const all = [...aPrices, ...bPrices];
    chartMin = Math.min(...all);
    chartMax = Math.max(...all);
    const pad = (chartMax - chartMin) * 0.08 || 100;
    chartMin -= pad;
    chartMax += pad;
}

function niceRange(min, max) {
    const span = max - min;
    const rough = span / 5;                       // ~5 grid ticks
    const mag = Math.pow(10, Math.floor(Math.log10(rough)));
    const res = rough / mag;
    let step;
    if (res <= 1.5) step = 1 * mag;
    else if (res <= 3.5) step = 2 * mag;
    else if (res <= 7.5) step = 5 * mag;
    else step = 10 * mag;
    const niceMin = Math.floor(min / step) * step;
    const niceMax = Math.ceil(max / step) * step;
    return { min: niceMin, max: niceMax, step };
}

function drawChart() {
    const dpr = window.devicePixelRatio || 1;
    const rect = chartCanvas.getBoundingClientRect();
    chartCanvas.width = rect.width * dpr;
    chartCanvas.height = rect.height * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const W = rect.width;
    const H = rect.height;
    const plotW = W - PAD.left - PAD.right;
    const plotH = H - PAD.top - PAD.bottom;

    // Merge both histories for the time axis
    const allPoints = [...historyA, ...historyB];
    if (!allPoints.length) return;

    const tMin = allPoints[0].t;
    const tMax = allPoints[allPoints.length - 1].t;
    const padT = Math.max(tMax - tMin, CHART_WINDOW_S * 500);
    const timeRange = [tMax - padT, tMax];

    // Price axis
    const priceSamples = allPoints.map(p => p.price);
    initChartScale(priceSamples, []);
    const pr = niceRange(chartMin, chartMax);
    chartMin = pr.min;
    chartMax = pr.max;

    const x = (t) => PAD.left + ((t - timeRange[0]) / (timeRange[1] - timeRange[0])) * plotW;
    const y = (p) => PAD.top + (1 - (p - chartMin) / (chartMax - chartMin)) * plotH;

    // Background
    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, W, H);

    // Grid lines & Y labels
    ctx.strokeStyle = COLORS.grid;
    ctx.lineWidth = 1;
    ctx.fillStyle = COLORS.label;
    ctx.font = "11px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";

    for (let v = pr.min; v <= pr.max + pr.step * 0.5; v += pr.step) {
        const yy = y(v);
        if (yy < PAD.top - 1 || yy > H - PAD.bottom + 1) continue;
        ctx.beginPath();
        ctx.moveTo(PAD.left, yy);
        ctx.lineTo(W - PAD.right, yy);
        ctx.stroke();
        ctx.fillText("$" + v.toLocaleString("en-US", { maximumFractionDigits: 0 }), PAD.left - 8, yy);
    }

    // X labels (time)
    const xStep = niceTickInterval(timeRange[1] - timeRange[0], 6);
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    for (let t0 = Math.ceil(timeRange[0] / xStep) * xStep; t0 <= timeRange[1]; t0 += xStep) {
        const xx = x(t0);
        if (xx < PAD.left || xx > W - PAD.right) continue;
        ctx.beginPath();
        ctx.moveTo(xx, PAD.top);
        ctx.lineTo(xx, H - PAD.bottom);
        ctx.stroke();
        const d = new Date(t0);
        const label = d.getHours().toString().padStart(2, "0") + ":" + d.getMinutes().toString().padStart(2, "0");
        ctx.fillText(label, xx, H - PAD.bottom + 8);
    }

    // Draw path helper (stroke only)
    function drawLine(data, color) {
        if (data.length < 2) return;
        ctx.strokeStyle = color;
        ctx.lineWidth = 2;
        ctx.lineJoin = "round";
        ctx.lineCap = "round";
        ctx.beginPath();
        for (let i = 0; i < data.length; i++) {
            const px = x(data[i].t);
            const py = y(data[i].price);
            i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py);
        }
        ctx.stroke();
    }

    // Draw path helper (area fill under the line)
    function drawArea(data, color) {
        if (data.length < 2) return;
        const grad = ctx.createLinearGradient(0, PAD.top, 0, H - PAD.bottom);
        grad.addColorStop(0, color.replace(")", ",0.18)").replace("rgb", "rgba"));
        grad.addColorStop(1, "rgba(0,0,0,0)");
        ctx.fillStyle = grad;
        ctx.beginPath();
        for (let i = 0; i < data.length; i++) {
            const px = x(data[i].t);
            const py = y(data[i].price);
            i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py);
        }
        ctx.lineTo(x(data[data.length - 1].t), H - PAD.bottom);
        ctx.lineTo(x(data[0].t), H - PAD.bottom);
        ctx.closePath();
        ctx.fill();
    }

    // Area fills (behind lines)
    drawArea(historyA, "rgb(255,107,107)");
    drawArea(historyB, "rgb(46,213,115)");

    // Lines
    drawLine(historyA, COLORS.ask);
    drawLine(historyB, COLORS.bid);

    // Legend (colored text in top-left of plot area)
    ctx.font = "bold 12px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
    ctx.textAlign = "left";
    ctx.textBaseline = "top";
    const legendX = PAD.left + 10;
    let ly = PAD.top + 8;
    ctx.fillStyle = COLORS.ask;
    ctx.fillText("▲ Avg Ask", legendX, ly);
    ctx.fillStyle = COLORS.bid;
    ctx.fillText("▼ Avg Bid", legendX, ly + 18);
}

function niceTickInterval(range, targetCount) {
    const rough = range / targetCount;
    const mag = Math.pow(10, Math.floor(Math.log10(rough)));
    const res = rough / mag;
    if (res <= 1.5) return 1 * mag;
    if (res <= 3.5) return 2 * mag;
    if (res <= 7.5) return 5 * mag;
    return 10 * mag;
}

// Redraw chart on resize; track mobile state for dynamic content
window.addEventListener("resize", drawChart);

let isMobile = window.innerWidth <= 640;
window.addEventListener("resize", () => { isMobile = window.innerWidth <= 640; });

// ── Mobile "Details" toggle — show/hide high & low price metrics ──
const detailsBtn = document.getElementById("detailsBtn");
const priceBox   = document.getElementById("priceBox");
if (detailsBtn && priceBox) {
    detailsBtn.addEventListener("click", () => {
        priceBox.classList.toggle("expanded");
    });
}

/**
 * Render one side of the order book with a cumulative total column.
 */
function renderSide(levels, element, cssClass) {
    // Sort asks ascending (lowest first), bids descending (highest first)
    const sorted = levels.map(l => [Number(l[0]), Number(l[1])]);
    if (cssClass === "ask") {
        sorted.sort((a, b) => a[0] - b[0]);
    } else {
        sorted.sort((a, b) => b[0] - a[0]);
    }

    let runningTotal = 0;
    let cumulativeUsdt = 0;

    // Pre-compute max level for depth bar scaling (O(n), not O(n²))
    const maxLevel = Math.max(...sorted.map(l => l[1]));

    element.innerHTML = "";

    for (const [price, amount] of sorted) {
        runningTotal += amount;
        cumulativeUsdt += price * amount;

        // Depth bar – normalise against the max level on this side
        const widthPct = (amount / maxLevel) * 100;

        const row = document.createElement("tr");
        row.className = cssClass;
        // On mobile (≤640px), hide the total column to gain space
        row.innerHTML = `
            <td class="price-cell">
                <span class="depth-bar" style="width:${widthPct}%"></span>
                ${formatPrice(price)}
            </td>
            <td class="amount-cell">${amount.toFixed(5)}</td>
            <td class="${isMobile ? 'total-cell no-total' : 'total-cell'}">${formatTotal(cumulativeUsdt)}</td>
        `;

        element.appendChild(row);
    }
}

function formatPrice(n) {
    return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatTotal(n) {
    if (n >= 1_000_000) return "$" + (n / 1_000_000).toFixed(3) + "M";
    if (n >= 1_000) return "$" + (n / 1_000).toFixed(2) + "K";
    return "$" + n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatTradeTotal(price, qty) {
    const total = price * qty;
    return formatTotal(total);
}
