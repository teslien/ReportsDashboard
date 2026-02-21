/**
 * Theme System — Light / Dark Mode
 * Reads/writes data-theme on <html>; injects the toggle button.
 */
(function () {
    const STORAGE_KEY = 'jira-dash-theme';

    // ── Initialise theme from storage / system preference ─────────
    function getInitialTheme() {
        const saved = localStorage.getItem(STORAGE_KEY);
        if (saved) return saved;
        return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
    }

    function applyTheme(theme) {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem(STORAGE_KEY, theme);

        const btn = document.getElementById('theme-toggle-btn');
        if (btn) {
            btn.title = theme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode';
            btn.innerHTML = theme === 'dark' ? '☀️' : '🌙';
        }

        // Update Chart.js charts if they exist (re-render with new colors)
        updateChartColors(theme);
    }

    function toggle() {
        const current = document.documentElement.getAttribute('data-theme') || 'dark';
        applyTheme(current === 'dark' ? 'light' : 'dark');
    }

    // ── Inject floating toggle button ─────────────────────────────
    function injectToggleButton() {
        if (document.getElementById('theme-toggle-btn')) return; // already exists
        const btn = document.createElement('button');
        btn.id = 'theme-toggle-btn';
        btn.setAttribute('aria-label', 'Toggle theme');
        btn.onclick = toggle;
        
        // Set initial state
        const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
        btn.title = currentTheme === 'dark' ? 'Switch to Light Mode' : 'Switch to Dark Mode';
        btn.innerHTML = currentTheme === 'dark' ? '☀️' : '🌙';

        document.body.appendChild(btn);
    }

    // ── Chart.js color adaption ───────────────────────────────────
    function updateChartColors(theme) {
        if (typeof Chart === 'undefined') return;
        const isLight = theme === 'light';
        const gridColor = isLight ? 'rgba(0,0,0,0.06)' : 'rgba(255,255,255,0.06)';
        const labelColor = isLight ? '#475569' : '#94a3b8';
        const legendColor = isLight ? '#1e293b' : '#e2e8f0';

        Chart.helpers.each(Chart.instances, function (chart) {
            // Grid lines
            if (chart.config.options?.scales) {
                Object.values(chart.config.options.scales).forEach(scale => {
                    if (scale.grid) scale.grid.color = gridColor;
                    if (scale.ticks) scale.ticks.color = labelColor;
                });
            }
            // Legend
            if (chart.config.options?.plugins?.legend?.labels) {
                chart.config.options.plugins.legend.labels.color = legendColor;
            }
            chart.update('none'); // no animation on theme switch
        });
    }

    // ── Run immediately so there's no flash of wrong theme ────────
    const initialTheme = getInitialTheme();
    applyTheme(initialTheme);

    // ── Wait for DOM to inject the button ─────────────────────────
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', injectToggleButton);
    } else {
        injectToggleButton();
    }

    // ── Expose globally for manual use ───────────────────────────
    window.themeToggle = { toggle, apply: applyTheme };
})();
