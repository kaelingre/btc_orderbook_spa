// ══════════════════════════════════════════════════════════
// App: BTC / USDT live order-book dashboard
// ══════════════════════════════════════════════════════════

// ── DOM element cache ──────────────────────────────────────
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

// ── Config constants ───────────────────────────────────────
const SAMPLE_INTERVAL_MS     = 1_000;         // chart sample rate (ms)
const X_AXIS_TARGET_LABELS   = [10, 6];       // [seconds-mode, minutes-mode] target label count

// ── State ──────────────────────────────────────────────────
let CHART_WINDOW_S            = 300;          // default window in seconds (matches HTML <option selected>)
let CHART_MIN                 = 0;          // running price-axis minimum
let CHART_MAX                 = 1;          // running price-axis maximum

// Chart history: [{ t, price }]
const historyA                = [];           // weighted average ask prices
const historyB                = [];           // weighted average bid prices
let lastSampleTime            = 0;            // throttling timestamp

// Ticker state (tracks direction for color)
const tickerState             = { _prev: 0, high24h: 0, low24h: 0, pctChange: 0 };

// Trades: dedup via tradeId
let tradeId                   = 0;
const MAX_TRADES              = 30;

// ── Canvas setup ───────────────────────────────────────────
const chartCanvas             = document.getElementById("chart");
const ctx                     = chartCanvas.getContext("2d");

// ══════════════════════════════════════════════════════════
// WebSocket connections (with auto-reconnect)
// ══════════════════════════════════════════════════════════

let depthConnected            = false;      // per-channel tracking for status badge
let secondaryConnected        = false;
let reconnectTimerDepth       = null;       // track pending reconnects so we don't stack
let reconnectTimerSecondary   = null;

/** Update the status badge based on individual channel health. */
function updateStatus() {
    if (!depthConnected && !secondaryConnected) {
        statusEl.className   = "status disconnected";
        statusEl.textContent = "Disconnected";
    } else if (depthConnected && secondaryConnected) {
        statusEl.className   = "status connected";
        statusEl.textContent = "Depth ✓  Trades ✓";
    } else {
        // Partial — show which channel is down
        const cls               = "connected";
        const pieces            = [];
        if (depthConnected)   pieces.push("Depth");
        if (secondaryConnected) pieces.push("Trades");
        statusEl.className     = cls;
        statusEl.textContent   = `${pieces.join(", ")} … reconnecting`;
    }
}

function connectDepth() {
    const socket                = new WebSocket(
        "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms"
    );

    socket.onopen               = () => {
        depthConnected          = true;
        updateStatus();         // combined status reflects current state of all channels
    };
    socket.onerror              = (err) => console.error("Depth socket error:", err);
    // Binance auto-reconnects before firing onclose — only schedule a reconnect if it's
    // a "real" close (no willReconnect flag). This avoids timer races.
    socket.onclose              = (event) => {
        depthConnected          = false;
        updateStatus();

        if (!event.willReconnect) {
            // Reconnect with 5s delay (no stacking — clear any pending timer)
            if (reconnectTimerDepth) clearTimeout(reconnectTimerDepth);
            reconnectTimerDepth     = setTimeout(connectDepth, 5_000);
        }
    };
    socket.onmessage            = (event) => {
        const data              = JSON.parse(event.data);
        renderSide(data.asks, asksEl, "ask");
        renderSide(data.bids, bidsEl, "bid");
        updateImbalance(data.asks, data.bids);

        // One chart sample per second (throttle the high-rate stream)
        const now             = Date.now();
        if (now - lastSampleTime >= SAMPLE_INTERVAL_MS) {
            pushSample(data.asks, data.bids);
            lastSampleTime  = now;
        }
    };

    return socket;              // returned so we can check readyState
}

function connectSecondary() {
    const socket                = new WebSocket(
        "wss://stream.binance.com:9443/stream?streams=btcusdt@ticker/btcusdt@trade"
    );

    socket.onopen               = () => {
        secondaryConnected      = true;
        updateStatus();         // combined status reflects current state of all channels
    };
    socket.onerror              = (err) => console.error("Secondary socket error:", err);
    socket.onclose              = (event) => {
        secondaryConnected      = false;
        updateStatus();

        if (reconnectTimerSecondary) clearTimeout(reconnectTimerSecondary);
        reconnectTimerSecondary = setTimeout(connectSecondary, 5_000);
    };
    socket.onmessage            = (event) => {
        const wrapper         = JSON.parse(event.data);
        const stream          = wrapper.stream;
        const data            = wrapper.data;
        if (!data) return;      // skip malformed messages

        if (stream === "btcusdt@ticker") updateTicker(data);
        if (stream === "btcusdt@trade")  addTrade(data);
    };

    return socket;
}

// ── Status badge helper (per-channel aware) ────────────────
let depthSocket       = connectDepth();
let secondarySocket   = connectSecondary();

function setStatus(className, text) {
    statusEl.className   = `status ${className}`;
    statusEl.textContent = text;
}

// ══════════════════════════════════════════════════════════
// Feature 1: Ticker / Current Price Info Box
// ══════════════════════════════════════════════════════════

function updateTicker(ticker) {
    // Binance @ticker fields: c=price, h=high, l=low, P=pct-change
    const current             = +ticker.c;
    const high                = +ticker.h;
    const low                 = +ticker.l;
    const changePct           = +ticker.P;

    if (isNaN(current)) return;

    // Direction: green if price rose, red if fell (first tick → neutral/no flash)
    const isInitialTick         = tickerState._prev === undefined;
    tickerState._prev           = isInitialTick ? current : tickerState._prev;
    tickerState.lastPrice       = current;

    const dir                   = isInitialTick || current >= tickerState._prev;
    currentPrice.className    = `price-current ${dir ? "up" : "down"}`;
    priceChange.style.color   = dir ? "#2ed573" : "#ff6b6b";

    currentPrice.textContent  = `$${formatPrice(current)}`;
    const sign                = changePct >= 0 ? "+" : "";
    priceChange.textContent   = `${sign}${changePct.toFixed(2)}%`;

    if (high > 0) priceHigh.textContent = `H: $${formatPrice(high)}`;
    if (low > 0)  priceLow.textContent  = `L: $${formatPrice(low)}`;
}

// ══════════════════════════════════════════════════════════
// Feature 2: Recent Trades Feed
// ══════════════════════════════════════════════════════════

const WHALE_THRESHOLD       = 0.5;   // BTC volume to flag as "whale"

function addTrade(trade) {
    const tId               = trade.t;
    if (tradeId !== 0 && tId <= tradeId) return;  // dedup / out-of-order trades
    tradeId                 = tId;

    const isBuy             = !trade.m;            // m=true → maker sell (ask filler)
    const trEl              = document.createElement("tr");
    trEl.className          = isBuy ? "bid" : "ask";

    // Format timestamp from Binance trade.T (epoch ms)
    // Note: Binance timestamps are second-precision, so milliseconds are always "000"
    const d                 = new Date(trade.T);
    const h                 = String(d.getHours()).padStart(2, "0");
    const m                 = String(d.getMinutes()).padStart(2, "0");
    const s                 = String(d.getSeconds()).padStart(2, "0");

    trEl.innerHTML          = `
        <td class="time-col">${h}:${m}:${s}</td>
        <td class="price-cell"><span>${formatPrice(+trade.p)}</span></td>
        <td class="amount-col whale-qty" style="font-weight:600">${Number(trade.q).toFixed(5)}</td>
        <td class="total-cell">$${formatTradeTotal(+trade.p, +trade.q)}</td>`;

    // Whale trade → pop out into overlay (held 10s)
    if (+trade.q >= WHALE_THRESHOLD) showWhale(trade);

    // Insert newest at top (null ref → appendChild behavior)
    tradesEl.insertBefore(trEl, tradesEl.firstChild || null);

    // Evict oldest rows beyond MAX_TRADES
    while (tradesEl.children.length > MAX_TRADES) {
        tradesEl.removeChild(tradesEl.lastChild);
    }
}

function showWhale(trade) {
    const isBuy             = !trade.m;

    whaleSide.textContent   = isBuy ? "BUY" : "SELL";
    whaleSide.style.color   = "#2ed573";
    whaleSide.style.background = "rgba(46,213,115,0.15)";

    const d                 = new Date(trade.T);
    whaleTime.textContent   = d.toUTCString().slice(-12, -4);
    whalePrice.textContent  = formatPrice(+trade.p);
    whaleAmount.textContent = `${Number(trade.q).toFixed(5)} BTC`;
    whaleTotal.textContent  = `$${formatTradeTotal(+trade.p, +trade.q)}`;

    // Force reflow so CSS animation restarts on every new whale
    whaleRow.classList.remove("fade-in", "fading-out");
    void whaleRow.offsetWidth;              // trigger reflow
    whaleRow.classList.add("fade-in");
    whaleOverlay.style.opacity = "1";

    // Timeline: 0s show → 8s fade out → 9.5s hide overlay
    setTimeout(() => {
        whaleRow.classList.remove("fade-in");
        whaleRow.classList.add("fading-out");
    }, 8_000);
    setTimeout(() => {
        whaleOverlay.style.opacity = "0";
    }, 9_500);
}

// ══════════════════════════════════════════════════════════
// Feature 3: Order Book Imbalance Bar
// ══════════════════════════════════════════════════════════

function updateImbalance(asks, bids) {
    // Guard against empty side — imbalance can't be computed
    if (!asks || !asks.length || !bids || !bids.length) return;

    let sellVol           = 0, buyVol           = 0;
    for (const [p, q] of asks) sellVol += +p * +q;
    for (const [p, q] of bids) buyVol   += +p * +q;

    const total             = sellVol + buyVol;
    if (total === 0) return;

    const buyPct            = (buyVol / total) * 100;

    // Position: 0% → far-left/Sell, 100% → far-right/Buy
    const trackWidth        = imbalanceFill.parentElement.clientWidth || 200;
    imbalanceFill.style.left = `${(buyPct / 100) * (trackWidth - 10)}px`;

    // Color: green → orange → red based on buy dominance
    if (buyPct >= 60) {
        imbalanceFill.style.background = "#2ed573";
        imbalancePct.style.color       = "#2ed573";
    } else if (buyPct <= 40) {
        imbalanceFill.style.background = "#ff6b6b";
        imbalancePct.style.color       = "#ff6b6b";
    } else {
        imbalanceFill.style.background = "#ffa502";
        imbalancePct.style.color       = "#ffa502";
    }

    imbalancePct.textContent = `${buyPct.toFixed(1)}%`;
}

// ══════════════════════════════════════════════════════════
// Chart data helpers
// ══════════════════════════════════════════════════════════

function pushSample(asks, bids) {
    const askAvg              = weightedAvg(asks);
    const bidAvg              = weightedAvg(bids);

    // Initialise chart scale on first sample
    if (askAvg > 0 && !historyA.length) initChartScale([askAvg], [bidAvg]);

    historyA.push({ t: Date.now(), price: askAvg });
    historyB.push({ t: Date.now(), price: bidAvg });

    evictOldData(CHART_WINDOW_S * 1000);
    drawChart();
}

/** Quantity-weighted average price from a levels array. */
function weightedAvg(levels) {
    let weightSum           = 0, pSum             = 0;
    for (const [pStr, qStr] of levels) {
        const p             = +pStr, q          = +qStr;
        pSum               += p * q;
        weightSum          += q;
    }
    return weightSum > 0 ? pSum / weightSum : 0;
}

function evictOldData(windowMs) {
    const cutoff            = Date.now() - windowMs;
    while (historyA.length && historyA[0].t < cutoff) historyA.shift();
    while (historyB.length && historyB[0].t < cutoff) historyB.shift();
}

// ══════════════════════════════════════════════════════════
// Chart rendering (Canvas 2D)
// ══════════════════════════════════════════════════════════

const CHART_PAD           = { top: 14, right: 24, bottom: 28, left: 72 };
const COLORS              = {
    ask:   "#ff6b6b",
    bid:   "#2ed573",
    grid:  "rgba(255,255,255,0.045)",
    label: "#6b7080",
    bg:    "#111927",
};

/** Initialise or reset the price-axis range from a set of samples. */
function initChartScale(aPrices, bPrices) {
    const all                 = [...aPrices, ...bPrices];
    CHART_MIN               = Math.min(...all);
    CHART_MAX               = Math.max(...all);
    const pad                 = (CHART_MAX - CHART_MIN) * 0.08 || 100;
    CHART_MIN              -= pad;
    CHART_MAX              += pad;
}

/** Compute "nice" grid tick range and step for a data span. */
function niceRange(min, max) {
    const span                = max - min;
    const rough               = span / 5;           // target ~5 ticks
    const mag                 = Math.pow(10, Math.floor(Math.log10(rough)));
    const res                 = rough / mag;
    let step;
    if (res <= 1.5)  step     = 1 * mag;
    else if (res <= 3.5) step = 2 * mag;
    else if (res <= 7.5) step = 5 * mag;
    else                     step = 10 * mag;
    return {
        min: Math.floor(min / step) * step,
        max: Math.ceil(max / step) * step,
        step,
    };
}

/** Draw the full price chart to the canvas. */
function drawChart() {
    const dpr                 = window.devicePixelRatio || 1;
    const rect                = chartCanvas.getBoundingClientRect();

    // Size canvas to device pixels for crisp rendering
    chartCanvas.width         = rect.width * dpr;
    chartCanvas.height        = rect.height * dpr;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const W                   = rect.width;
    const H                   = rect.height;
    const plotW               = W - CHART_PAD.left - CHART_PAD.right;
    const plotH               = H - CHART_PAD.top  - CHART_PAD.bottom;

    // --- Time axis: always exactly CHART_WINDOW_S wide, anchored to now ---
    const allPoints           = [...historyA, ...historyB];
    if (!allPoints.length) return;

    const tMax                = allPoints[allPoints.length - 1].t;
    const timeRange           = [tMax - CHART_WINDOW_S * 1000, tMax];

    // --- Price axis ---
    const priceSamples        = allPoints.map(p => p.price);
    initChartScale(priceSamples, []);
    const pr                  = niceRange(CHART_MIN, CHART_MAX);
    CHART_MIN               = pr.min;
    CHART_MAX               = pr.max;

    // Coordinate maps (defined once, reused throughout this draw)
    const x                   = (t) => CHART_PAD.left + ((t - timeRange[0]) / (timeRange[1] - timeRange[0])) * plotW;
    const y                   = (p) => CHART_PAD.top  + (1 - (p - CHART_MIN) / (CHART_MAX - CHART_MIN))             * plotH;

    // --- Background ---
    ctx.fillStyle             = COLORS.bg;
    ctx.fillRect(0, 0, W, H);

    // --- Grid lines & Y-axis labels ---
    drawYGrid(y, pr, W, H);

    // --- X-axis labels (time) -- adaptive format & spacing for mobile ---
    drawXAxis(timeRange, x, plotW, W, H);

    // --- Data layers: area → line → legend ---
    drawAreaFill(historyA, "rgb(255,107,107)", x, y, rect.height);
    drawAreaFill(historyB, "rgb(46,213,115)", x, y, rect.height);
    drawLinePath(historyA, COLORS.ask, x, y);
    drawLinePath(historyB, COLORS.bid, x, y);
    drawLegend(CHART_PAD.left + 10, CHART_PAD.top + 8);
}

/** Draw horizontal grid lines and Y-axis price labels. */
function drawYGrid(yFn, range, canvasW, canvasH) {
    ctx.strokeStyle           = COLORS.grid;
    ctx.lineWidth             = 1;
    ctx.fillStyle             = COLORS.label;
    ctx.font                  = "11px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
    ctx.textAlign             = "right";
    ctx.textBaseline          = "middle";

    for (let v                 = range.min; v <= range.max + range.step * 0.5; v += range.step) {
        const yy                = yFn(v);
        if (yy < CHART_PAD.top - 1 || yy > canvasH - CHART_PAD.bottom + 1) continue;

        ctx.beginPath();
        ctx.moveTo(CHART_PAD.left, yy);
        ctx.lineTo(canvasW - CHART_PAD.right, yy);
        ctx.stroke();

        // Grid-line labels are "nice" reference values (rounded to nearest $) — not exact prices.
        ctx.fillText(
            `$${v.toLocaleString("en-US", { maximumFractionDigits: 0 })}`,
            CHART_PAD.left - 8, yy
        );
    }
}

/** Draw X-axis time labels, adapting format & spacing for mobile narrow screens. */
function drawXAxis(timeRange, xFn, plotW, canvasW, canvasH) {
    const spanMs              = timeRange[1] - timeRange[0];

    // Use seconds (HH:MM:SS) for small windows; HH:MM only for >=30min windows.
    let useSecond             = CHART_WINDOW_S <= 600;

    // On narrow screens in seconds-mode, switch to HH:MM with wider spacing
    // to avoid label crowding (typical ~72px font width at 11px / ~58px at 9px).
    if (useSecond && plotW < 420) {
        useSecond             = false;
    }

    // Pick a "nice" interval that gives enough labels for the available space.
    const targetInterval      = spanMs / X_AXIS_TARGET_LABELS[useSecond ? 0 : 1];
    const iterStep            = niceNumberStep(targetInterval);

    ctx.textAlign             = "center";
    ctx.textBaseline          = "top";

    let lastLabel             = null;

    for (let t0                = Math.ceil(timeRange[0] / iterStep) * iterStep;
         t0 <= timeRange[1];
         t0 += iterStep) {

        const xx                = xFn(t0);
        if (xx < CHART_PAD.left || xx > canvasW - CHART_PAD.right) continue;

        // Format label: HH:MM:SS or HH:MM depending on resolution mode.
        const d                 = new Date(t0);
        const hStr              = String(d.getHours()).padStart(2, "0");
        const mStr              = String(d.getMinutes()).padStart(2, "0");
        const sStr              = useSecond ? String(d.getSeconds()).padStart(2, "0") : "";
        const label             = useSecond ? `${hStr}:${mStr}:${sStr}` : `${hStr}:${mStr}`;

        // Deduplicate: skip if this time slot would show the same label.
        if (lastLabel !== null && label === lastLabel) continue;
        lastLabel             = label;

        // Grid lines in seconds-mode or when step is sparse enough.
        if (useSecond || iterStep > spanMs / 6) {
            ctx.beginPath();
            ctx.moveTo(xx, CHART_PAD.top);
            ctx.lineTo(xx, canvasH - CHART_PAD.bottom);
            ctx.stroke();
        }

        ctx.fillText(label, xx, canvasH - CHART_PAD.bottom + 8);
    }
}

/** Compute a "nice" number step from a target (like niceRange for prices, but returns a single interval). */
function niceNumberStep(target) {
    const mag                 = Math.pow(10, Math.floor(Math.log10(target)));
    const res                 = target / mag;
    if (res <= 1.5) return          1 * mag;
    if (res <= 3.5) return          2 * mag;
    if (res <= 7.5) return          5 * mag;
    return                         10 * mag;
}

/** Draw area fill under a data series with gradient. */
function drawAreaFill(data, colorStr, xFn, yFn, plotBottom) {
    if (data.length < 2) return;

    // Gradient: top → fully transparent at plot bottom.
    const grad                = ctx.createLinearGradient(0, CHART_PAD.top, 0, plotBottom);
    const rgbaColor           = colorStr.replace(")", ",0.18)").replace("rgb", "rgba");
    grad.addColorStop(0, rgbaColor);
    grad.addColorStop(1, "rgba(0,0,0,0)");

    ctx.fillStyle             = grad;
    ctx.beginPath();

    for (let i                 = 0; i < data.length; i++) {
        const px              = xFn(data[i].t);
        const py              = yFn(data[i].price);
        if (i === 0) ctx.moveTo(px, py);
        else         ctx.lineTo(px, py);
    }

    // Close path down to plot baseline.
    ctx.lineTo(xFn(data[data.length - 1].t), plotBottom);
    ctx.lineTo(xFn(data[0].t),               plotBottom);
    ctx.closePath();
    ctx.fill();
}

/** Draw a single data series as a line on top of the area fill. */
function drawLinePath(data, color, xFn, yFn) {
    if (data.length < 2) return;

    ctx.strokeStyle           = color;
    ctx.lineWidth             = 2;
    ctx.lineJoin              = "round";
    ctx.lineCap               = "round";
    ctx.beginPath();

    for (let i                 = 0; i < data.length; i++) {
        const px              = xFn(data[i].t);
        const py              = yFn(data[i].price);
        if (i === 0) ctx.moveTo(px, py);
        else         ctx.lineTo(px, py);
    }
    ctx.stroke();
}

/** Draw legend in the top-left of the plot area. */
function drawLegend(legendX, ly) {
    ctx.font                = "bold 12px -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";
    ctx.textAlign           = "left";
    ctx.textBaseline        = "top";

    ctx.fillStyle           = COLORS.ask;
    ctx.fillText("▲ Avg Ask", legendX, ly);
    ctx.fillStyle           = COLORS.bid;
    ctx.fillText("▼ Avg Bid", legendX, ly + 18);
}

// ══════════════════════════════════════════════════════════
// Order book rendering
// ══════════════════════════════════════════════════════════

/** Render one side of the order book with depth bar and cumulative total. */
function renderSide(levels, element, cssClass) {
    // Guard against empty side (can happen during init or temporary no-orders)
    if (!levels || !levels.length) return;

    // Normalise to [price, qty] number arrays
    const sorted            = levels.map(l => [Number(l[0]), Number(l[1])]);
    // asks: ascending price (lowest sell first); bids: descending price (highest buy first)
    if (cssClass === "ask") {
        sorted.sort((a, b) => a[0] - b[0]);
    } else {
        sorted.sort((a, b) => b[0] - a[0]);
    }

    // Single-pass: find max depth, compute cumulative total, build rows
    const maxDepth            = Math.max(...sorted.map(l => l[1]));
    let runningTotal          = 0;
    let cumulativeUsdt        = 0;

    element.innerHTML         = "";   // clear old content

    for (const [price, amount] of sorted) {
        runningTotal       += amount;
        cumulativeUsdt    += price * amount;

        const depthWidth    = (amount / maxDepth) * 100;   // % of max on this side

        const row           = document.createElement("tr");
        row.className       = cssClass;
        row.innerHTML       = `
            <td class="price-cell">
                <span class="depth-bar" style="width:${depthWidth}%"></span>
                ${formatPrice(price)}
            </td>
            <td class="amount-cell">${amount.toFixed(5)}</td>
            <td class="total-cell">${formatTotal(cumulativeUsdt)}</td>`;

        element.appendChild(row);
    }
}

// ══════════════════════════════════════════════════════════
// Utility helpers
// ══════════════════════════════════════════════════════════

function formatPrice(n) {
    return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatTotal(n) {
    if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(3)}M`;
    if (n >= 1_000)     return `$${(n / 1_000).toFixed(2)}K`;
    return `$${n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatTradeTotal(price, qty) {
    return formatTotal(price * qty);
}

// ══════════════════════════════════════════════════════════
// Event listeners (non-critical UI features)
// ══════════════════════════════════════════════════════════

// ── Chart redraw on resize (debounced via rAF — no double-draws) ──
let resizeRaf                 = null;
window.addEventListener("resize", () => {
    if (resizeRaf) cancelAnimationFrame(resizeRaf);
    resizeRaf                   = requestAnimationFrame(drawChart);
});

// ── Time-window selector with localStorage persistence ──
const timeWindowEl            = document.getElementById("time-window");
const saved                 = localStorage.getItem("chartWindow");
if (saved) {
    CHART_WINDOW_S          = Number(saved);        // restore last-used window
    timeWindowEl.value      = saved;                // sync <select> to match
}

timeWindowEl.addEventListener("change", (e) => {
    const newWindow           = Number(e.target.value);
    evictOldData(newWindow * 1000);
    CHART_WINDOW_S          = newWindow;
    localStorage.setItem("chartWindow", String(newWindow));
    drawChart();
});

// ── Mobile "Details" toggle (show/hide high & low price metrics) ──
const detailsBtn              = document.getElementById("detailsBtn");
const priceBoxEl              = document.getElementById("priceBox");
if (detailsBtn && priceBoxEl) {
    detailsBtn.addEventListener("click", () => {
        priceBoxEl.classList.toggle("expanded");
    });
}
