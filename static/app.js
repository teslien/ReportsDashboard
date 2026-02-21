let chart;

function getProjectKey() {
    return (localStorage.getItem("project_key") || "").trim().toUpperCase();
}

function getJiraEmail() {
    return (localStorage.getItem("jira_email") || "").trim();
}

function getJiraToken() {
    return (localStorage.getItem("jira_token") || "").trim();
}

function getApiHeaders() {
    return {
        "Content-Type": "application/json",
        "X-Project-Key": getProjectKey(),
        "X-Jira-Email": getJiraEmail(),
        "X-Jira-Token": getJiraToken()
    };
}

async function init() {
    await loadAssignees();
}

async function loadAssignees() {
    const projectKey = getProjectKey();
    if (!projectKey) return;
    const res = await fetch("/api/assignees", {
        method: "POST",
        headers: getApiHeaders(),
        body: JSON.stringify({
            jql: `project = ${projectKey} AND assignee IS NOT EMPTY`,
            maxResults: 100,
            fields: ["assignee"]
        })
    });

    const data = await res.json();
    const map = new Map();

    data.issues.forEach(i => {
        const a = i.fields.assignee;
        if (a) map.set(a.accountId, a.displayName);
    });

    // Convert to array, format names, and sort
    const sortedAssignees = Array.from(map, ([id, name]) => {
        // Format name: replace dots with spaces and capitalize each word
        const formattedName = name.replace(/\./g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        return { id, name: formattedName };
    }).sort((a, b) => a.name.localeCompare(b.name));

    const select = document.getElementById("assignee");
    if (select) { // Only running on dashboard page
        select.innerHTML = "<option value=''>Select assignee</option>";

        sortedAssignees.forEach(assignee => {
            const opt = document.createElement("option");
            opt.value = assignee.id;
            opt.textContent = assignee.name;
            select.appendChild(opt);
        });
    }
}

function buildJql(accountId, range) {
    const projectKey = getProjectKey();
    if (!projectKey) return "";
    let date = "";
    if (range === "day") date = "resolutiondate >= startOfDay()";
    if (range === "week") date = "resolutiondate >= startOfWeek()";
    if (range === "month") date = "resolutiondate >= startOfMonth()";
    if (range === "year") date = "resolutiondate >= startOfYear()";

    return `
    project = ${projectKey}
    AND statusCategory = Done
    AND assignee = ${accountId}
    AND ${date}
  `;
}

async function loadProductivity() {
    const assignee = document.getElementById("assignee").value;
    const range = document.getElementById("range").value;
    const projectKey = getProjectKey();

    if (!assignee) return alert("Select assignee");

    const jql = buildJql(assignee, range);
    if (!jql) return alert("Set Project Key in Settings first.");

    const res = await fetch("/api/search", {
        method: "POST",
        headers: getApiHeaders(),
        body: JSON.stringify({
            jql,
            maxResults: 1000,
            fields: ["key"]
        })
    });

    const data = await res.json();
    document.getElementById("count").innerText = data.issues.length;
    renderChart(range, data.issues.length);
}

function renderChart(range, count) {
    const ctx = document.getElementById("chart");
    if (chart) chart.destroy();

    chart = new Chart(ctx, {
        type: "bar",
        data: {
            labels: [range.toUpperCase()],
            datasets: [{
                label: "Tickets Closed",
                data: [count],
                backgroundColor: "#2563eb"
            }]
        },
        options: {
            scales: { y: { beginAtZero: true } }
        }
    });
}

// =========================
// SCOREBOARD LOGIC
// =========================

async function loadScoreboardData() {
    const range = document.getElementById("range").value;

    const leaderboardEl = document.getElementById("leaderboard");
    if (leaderboardEl) leaderboardEl.innerHTML = '<div class="text-gray-400 text-center p-4">Loading stats...</div>';

    try {
        const projectKey = getProjectKey();
        const res = await fetch("/api/scoreboard_data?t=" + new Date().getTime(), {
            method: "POST",
            headers: getApiHeaders(),
            body: JSON.stringify({ range })
        });

        const data = await res.json();
        console.log("Scoreboard Data:", data);

        // Sort by count descending
        data.sort((a, b) => b.count - a.count);

        if (leaderboardEl) {
            renderLeaderboard(data);
        }

        // Render Charts
        renderComparisonChart(data);

        // Render Velocity Trend
        loadVelocityData(range);

    } catch (e) {
        console.error(e);
        if (leaderboardEl) leaderboardEl.innerHTML = `<div class="text-red-400 text-center">Error loading data</div>`;
    }
}

async function loadVelocityData(range) {
    try {
        const projectKey = getProjectKey();
        const res = await fetch("/api/velocity_data", {
            method: "POST",
            headers: getApiHeaders(),
            body: JSON.stringify({ range })
        });
        const data = await res.json();
        renderVelocityChart(data, range);
    } catch (e) {
        console.error("Velocity error:", e);
    }
}

function renderVelocityChart(data, range) {
    const ctx = document.getElementById("trendChart");
    if (!ctx) return;

    if (window.trendChart instanceof Chart) window.trendChart.destroy();

    let labels = data.map(d => d.date);
    const counts = data.map(d => d.count);

    window.trendChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Completed Tickets',
                data: counts,
                borderColor: '#10b981', // Emerald 500
                backgroundColor: 'rgba(16, 185, 129, 0.2)',
                tension: 0.4,
                fill: true,
                pointRadius: 4,
                pointBackgroundColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(0,0,0,0.8)',
                    titleColor: '#fff',
                    bodyColor: '#fff',
                    callbacks: {
                        title: function (context) {
                            return context[0].label;
                        }
                    }
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(255,255,255,0.1)' },
                    ticks: { color: '#9ca3af', stepSize: 1 }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#9ca3af', maxTicksLimit: 10 }
                }
            }
        }
    });
}

function renderLeaderboard(data) {
    const container = document.getElementById("leaderboard");
    container.innerHTML = "";

    // Get current filter
    const currentRange = document.getElementById("range")?.value || "year";

    data.forEach((user, index) => {
        const rank = index + 1;
        let medal = "";
        if (rank === 1) medal = "👑";
        else if (rank === 2) medal = "🥈";
        else if (rank === 3) medal = "🥉";
        else medal = `<span class="font-mono text-gray-500">#${rank}</span>`;

        // Format Name
        const name = user.name.replace(/\./g, ' ').replace(/\b\w/g, c => c.toUpperCase());

        // Changed to Anchor tag for linking with range param
        const card = document.createElement("a");
        card.href = `/user/${user.id}?range=${currentRange}`;
        card.className = "flex items-center gap-4 bg-gray-700/50 p-4 rounded-xl border border-gray-600 hover:bg-gray-700 transition transform hover:-translate-y-1 cursor-pointer no-underline group";
        card.innerHTML = `
            <div class="text-xl font-bold w-8 text-center">${medal}</div>
            <img src="${user.avatar}" class="w-10 h-10 rounded-full border-2 border-gray-500 group-hover:border-blue-400 transition">
            <div class="flex-1">
                <h4 class="font-bold text-gray-200 group-hover:text-white transition">${name}</h4>
                <div class="text-xs text-blue-400 group-hover:text-blue-300 transition">${user.count} tickets closed</div>
            </div>
            <div class="text-2xl font-black text-white group-hover:scale-110 transition">${user.count}</div>
        `;
        container.appendChild(card);
    });
}

// =========================
// USER DETAILS LOGIC
// =========================
async function loadUserTickets(accountId) {
    const tableBody = document.getElementById("ticketTable");

    // Get range: 1. from URL (priority on load), 2. default "month"
    const urlParams = new URLSearchParams(window.location.search);
    let range = urlParams.get('range') || "month";
    let specificDate = urlParams.get('date') || "";

    // Sync dropdown and date UI if exists
    const rangeSelect = document.getElementById("range");
    const specificDateInput = document.getElementById("specificDate");
    const specificDateWrapper = document.getElementById("specificDateWrapper");

    if (rangeSelect) rangeSelect.value = range;
    if (specificDateInput) specificDateInput.value = specificDate;
    if (specificDateWrapper) {
        if (range === "date") specificDateWrapper.classList.remove("hidden");
        else specificDateWrapper.classList.add("hidden");
    }

    try {
        const projectKey = getProjectKey();
        const res = await fetch("/api/user_tickets", {
            method: "POST",
            headers: getApiHeaders(),
            body: JSON.stringify({ accountId, range, specificDate })
        });

        const issues = await res.json();

        if (issues.length === 0) {
            const rangeDisplay = range === "date" ? `on ${specificDate}` : `for this ${range}`;
            tableBody.innerHTML = `<tr><td colspan="6" class="p-8 text-center text-gray-400">No tickets found ${rangeDisplay}.</td></tr>`;
            return;
        }

        // Update Name from first issue if available
        if (issues[0]?.fields?.assignee?.displayName) {
            const rawName = issues[0].fields.assignee.displayName;
            document.getElementById("userName").innerText = rawName.replace(/\./g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        }

        tableBody.innerHTML = "";

        issues.forEach(i => {
            const f = i.fields;
            const date = new Date(f.resolutiondate).toLocaleDateString();
            const icon = f.issuetype?.iconUrl ? `<img src="${f.issuetype.iconUrl}" class="w-4 h-4" title="${f.issuetype.name}">` : "📄";
            const priority = f.priority?.name || "None";
            const priorityColor = priority === "High" || priority === "Highest" ? "text-red-400" : "text-gray-400";
            const summary = f.summary || "No Summary";

            const row = document.createElement("tr");
            row.className = "hover:bg-gray-700/50 transition border-b border-gray-700 last:border-0";
            row.innerHTML = `
                <td class="p-4 py-3">${icon}</td>
                <td class="p-4 py-3 font-mono text-blue-300 hover:underline"><a href="https://lumberfi.atlassian.net/browse/${i.key}" target="_blank">${i.key}</a></td>
                <td class="p-4 py-3 font-medium text-white">${summary}</td>
                <td class="p-4 py-3 ${priorityColor}">${priority}</td>
                <td class="p-4 py-3 text-gray-400 text-xs">${date}</td>
                <td class="p-4 py-3">
                    <a href="https://lumberfi.atlassian.net/browse/${i.key}" target="_blank" 
                       class="text-xs bg-blue-600 hover:bg-blue-500 text-white px-3 py-1.5 rounded transition">
                       View in Jira
                    </a>
                </td>
            `;
            tableBody.appendChild(row);
        });

    } catch (e) {
        console.error(e);
        tableBody.innerHTML = '<tr><td colspan="6" class="p-8 text-center text-red-400">Error loading tickets.</td></tr>';
    }
}

function renderComparisonChart(data) {
    const ctx = document.getElementById("comparisonChart");
    if (!ctx) return;

    // Destroy old chart instance if exists
    if (window.compChart instanceof Chart) window.compChart.destroy();

    const names = data.map(d => d.name.split(' ')[0]); // First names only for cleaner chart
    const counts = data.map(d => d.count);

    window.compChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: names,
            datasets: [{
                label: 'Tickets Closed',
                data: counts,
                backgroundColor: 'rgba(59, 130, 246, 0.8)',
                borderColor: '#60a5fa',
                borderWidth: 1,
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: 'rgba(0,0,0,0.8)',
                    titleColor: '#fff',
                    bodyColor: '#fff'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { color: 'rgba(255,255,255,0.1)' },
                    ticks: { color: '#9ca3af' }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: '#9ca3af' }
                }
            }
        }
    });
}
