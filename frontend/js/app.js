const WS_URL = `ws://${window.location.host}/ws`;

let timelineChart, productsChart, revenueChart;
let reconnectAttempts = 0;
const MAX_RECONNECT_DELAY = 10000;

const CHART_COLORS = [
    '#6c63ff', '#34d399', '#fbbf24', '#f87171', '#60a5fa',
    '#a78bfa', '#f472b6', '#fb923c', '#2dd4bf', '#818cf8',
];

function initCharts() {
    const defaultOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                labels: { color: '#9aa0b0', font: { size: 11 } }
            }
        },
    };

    timelineChart = new Chart(document.getElementById('timelineChart'), {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Orders',
                data: [],
                borderColor: '#6c63ff',
                backgroundColor: 'rgba(108, 99, 255, 0.1)',
                fill: true,
                tension: 0.4,
                pointRadius: 3,
                pointBackgroundColor: '#6c63ff',
            }]
        },
        options: {
            ...defaultOptions,
            scales: {
                x: {
                    ticks: { color: '#9aa0b0', maxTicksLimit: 10, font: { size: 10 } },
                    grid: { color: 'rgba(42, 47, 69, 0.5)' },
                },
                y: {
                    beginAtZero: true,
                    ticks: { color: '#9aa0b0', stepSize: 1, font: { size: 10 } },
                    grid: { color: 'rgba(42, 47, 69, 0.5)' },
                }
            },
            plugins: {
                ...defaultOptions.plugins,
                legend: { display: false },
            }
        }
    });

    productsChart = new Chart(document.getElementById('productsChart'), {
        type: 'doughnut',
        data: {
            labels: [],
            datasets: [{
                data: [],
                backgroundColor: CHART_COLORS,
                borderColor: '#1e2235',
                borderWidth: 2,
            }]
        },
        options: {
            ...defaultOptions,
            plugins: {
                legend: {
                    position: 'right',
                    labels: { color: '#9aa0b0', font: { size: 11 }, padding: 12 }
                }
            }
        }
    });

    revenueChart = new Chart(document.getElementById('revenueChart'), {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Revenue ($)',
                data: [],
                backgroundColor: CHART_COLORS.map(c => c + '99'),
                borderColor: CHART_COLORS,
                borderWidth: 1,
                borderRadius: 6,
            }]
        },
        options: {
            ...defaultOptions,
            indexAxis: 'y',
            scales: {
                x: {
                    beginAtZero: true,
                    ticks: { color: '#9aa0b0', font: { size: 10 } },
                    grid: { color: 'rgba(42, 47, 69, 0.5)' },
                },
                y: {
                    ticks: { color: '#9aa0b0', font: { size: 11 } },
                    grid: { display: false },
                }
            },
            plugins: {
                ...defaultOptions.plugins,
                legend: { display: false },
            }
        }
    });
}

function updateStats(stats) {
    animateValue('totalOrders', stats.total_orders);
    animateValue('totalItems', stats.total_items);

    document.getElementById('totalRevenue').textContent = `$${stats.total_revenue.toFixed(2)}`;
    const avg = stats.total_orders > 0
        ? (stats.total_revenue / stats.total_orders).toFixed(2)
        : '0.00';
    document.getElementById('avgOrder').textContent = `$${avg}`;

    // Timeline
    if (stats.orders_timeline && stats.orders_timeline.length > 0) {
        timelineChart.data.labels = stats.orders_timeline.map(t => t.time);
        timelineChart.data.datasets[0].data = stats.orders_timeline.map(t => t.count);
        timelineChart.update('none');
    }

    // Products doughnut
    const products = stats.orders_per_product || {};
    const sortedProducts = Object.entries(products).sort((a, b) => b[1] - a[1]);
    productsChart.data.labels = sortedProducts.map(([k]) => k);
    productsChart.data.datasets[0].data = sortedProducts.map(([, v]) => v);
    productsChart.update('none');

    // Revenue bar
    const revenue = stats.revenue_per_product || {};
    const sortedRevenue = Object.entries(revenue).sort((a, b) => b[1] - a[1]);
    revenueChart.data.labels = sortedRevenue.map(([k]) => k);
    revenueChart.data.datasets[0].data = sortedRevenue.map(([, v]) => v);
    revenueChart.update('none');
}

function animateValue(elementId, value) {
    const el = document.getElementById(elementId);
    el.textContent = value.toLocaleString();
    el.classList.add('animate');
    setTimeout(() => el.classList.remove('animate'), 300);
}

function addOrderToFeed(order) {
    const list = document.getElementById('ordersList');
    const empty = list.querySelector('.empty-state');
    if (empty) empty.remove();

    const item = document.createElement('div');
    item.className = 'order-item';
    item.innerHTML = `
        <div class="order-info">
            <span class="order-product">${order.quantity}x ${capitalize(order.item)}</span>
            <span class="order-details">${capitalize(order.user)} &bull; ${formatTime(order.timestamp)}</span>
        </div>
        <span class="order-price">$${order.revenue.toFixed(2)}</span>
    `;

    list.insertBefore(item, list.firstChild);

    // Keep max 50 items
    while (list.children.length > 50) {
        list.removeChild(list.lastChild);
    }
}

function loadOrders(orders) {
    const list = document.getElementById('ordersList');
    list.innerHTML = '';
    if (!orders || orders.length === 0) {
        list.innerHTML = '<div class="empty-state">Waiting for orders...</div>';
        return;
    }
    orders.forEach(order => {
        const item = document.createElement('div');
        item.className = 'order-item';
        item.style.animation = 'none';
        item.innerHTML = `
            <div class="order-info">
                <span class="order-product">${order.quantity}x ${capitalize(order.item)}</span>
                <span class="order-details">${capitalize(order.user)} &bull; ${formatTime(order.timestamp)}</span>
            </div>
            <span class="order-price">$${order.revenue.toFixed(2)}</span>
        `;
        list.appendChild(item);
    });
}

function capitalize(str) {
    if (!str) return '';
    return str.charAt(0).toUpperCase() + str.slice(1);
}

function formatTime(ts) {
    if (!ts) return '';
    const d = new Date(ts);
    return d.toLocaleTimeString();
}

function setConnectionStatus(connected) {
    const el = document.getElementById('connectionStatus');
    const dot = el.querySelector('.status-dot');
    const text = el.querySelector('span:last-child');

    if (connected) {
        dot.className = 'status-dot connected';
        text.textContent = 'Connected';
    } else {
        dot.className = 'status-dot disconnected';
        text.textContent = 'Disconnected';
    }
}

function connectWebSocket() {
    const ws = new WebSocket(WS_URL);

    ws.onopen = () => {
        setConnectionStatus(true);
        reconnectAttempts = 0;
    };

    ws.onmessage = (event) => {
        const data = JSON.parse(event.data);

        if (data.type === 'init') {
            loadOrders(data.orders);
            updateStats(data.stats);
        } else if (data.type === 'new_order') {
            addOrderToFeed(data.order);
            updateStats(data.stats);
        }
    };

    ws.onclose = () => {
        setConnectionStatus(false);
        const delay = Math.min(1000 * Math.pow(2, reconnectAttempts), MAX_RECONNECT_DELAY);
        reconnectAttempts++;
        setTimeout(connectWebSocket, delay);
    };

    ws.onerror = () => {
        ws.close();
    };
}

document.addEventListener('DOMContentLoaded', () => {
    initCharts();
    connectWebSocket();
});
