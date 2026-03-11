/* ========================================
   Blog Riesgos y Desastres - Main App JS
   ======================================== */

// Storage keys
const STORAGE_KEY = 'blog_riesgos_articles';

// ---- Data helpers ----
function getArticles() {
    try {
        return JSON.parse(localStorage.getItem(STORAGE_KEY)) || [];
    } catch {
        return [];
    }
}

function getArticleById(id) {
    return getArticles().find(a => a.id === id);
}

// ---- Rendering helpers ----
function formatDate(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr + 'T00:00:00');
    const months = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
    return `${d.getDate()} de ${months[d.getMonth()]} de ${d.getFullYear()}`;
}

function createArticleCard(article) {
    const card = document.createElement('article');
    card.className = 'article-card';

    const imageHtml = article.image
        ? `<img src="${escapeHtml(article.image)}" alt="${escapeHtml(article.title)}" class="article-card-image" loading="lazy">`
        : `<div class="article-card-image img-placeholder">&#128240;</div>`;

    const tags = (article.tags || []).map(t => `<span class="tag">${escapeHtml(t)}</span>`).join('');

    const shareUrl = encodeURIComponent(window.location.origin + '/articulo.html?id=' + article.id);
    const shareTitle = encodeURIComponent(article.title);
    const linkedinShare = `https://www.linkedin.com/sharing/share-offsite/?url=${shareUrl}`;

    card.innerHTML = `
        ${imageHtml}
        <div class="article-card-body">
            <div class="article-card-meta">
                <span class="date">&#128197; ${formatDate(article.date)}</span>
                <span class="version">${escapeHtml(article.version || 'v1.0')}</span>
            </div>
            <h3><a href="articulo.html?id=${article.id}">${escapeHtml(article.title)}</a></h3>
            <p class="article-card-excerpt">${escapeHtml(article.summary || '')}</p>
            <div class="article-card-tags">${tags}</div>
            <div class="article-card-footer">
                <a href="articulo.html?id=${article.id}" class="read-more">Leer mas &rarr;</a>
                <a href="${linkedinShare}" target="_blank" rel="noopener" class="share-btn" title="Compartir en LinkedIn">
                    &#x1F517; LinkedIn
                </a>
            </div>
        </div>
    `;
    return card;
}

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// ---- Toast notifications ----
function showToast(message) {
    let toast = document.getElementById('toast');
    if (!toast) {
        toast = document.createElement('div');
        toast.id = 'toast';
        toast.className = 'toast';
        document.body.appendChild(toast);
    }
    toast.textContent = message;
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 3000);
}

// ---- Mobile menu ----
function initMobileMenu() {
    const toggle = document.querySelector('.menu-toggle');
    const nav = document.querySelector('.nav-links');
    if (toggle && nav) {
        toggle.addEventListener('click', () => {
            nav.classList.toggle('open');
            toggle.setAttribute('aria-expanded', nav.classList.contains('open'));
        });
    }
}

// ---- Home Page ----
function initHomePage() {
    const articles = getArticles().sort((a, b) => new Date(b.date) - new Date(a.date));

    // Featured article
    const featuredEl = document.getElementById('featured-article');
    if (featuredEl && articles.length > 0) {
        const feat = articles[0];
        const shareUrl = encodeURIComponent(window.location.origin + '/articulo.html?id=' + feat.id);
        const linkedinShare = `https://www.linkedin.com/sharing/share-offsite/?url=${shareUrl}`;

        featuredEl.innerHTML = `
            <div>
                <span class="featured-badge">Destacado</span>
                <h2>${escapeHtml(feat.title)}</h2>
                <div class="article-card-meta">
                    <span class="date">&#128197; ${formatDate(feat.date)}</span>
                    <span class="version">${escapeHtml(feat.version || 'v1.0')}</span>
                </div>
                <p>${escapeHtml(feat.summary || '')}</p>
                <div style="display:flex;gap:1rem;flex-wrap:wrap;">
                    <a href="articulo.html?id=${feat.id}" class="btn btn-primary">Leer articulo</a>
                    <a href="${linkedinShare}" target="_blank" rel="noopener" class="btn btn-outline">&#x1F517; Compartir</a>
                </div>
            </div>
            <div>
                ${feat.image ? `<img src="${escapeHtml(feat.image)}" alt="${escapeHtml(feat.title)}">` : '<div class="img-placeholder" style="height:280px;border-radius:8px;">&#128240;</div>'}
            </div>
        `;
        featuredEl.style.display = 'grid';
    } else if (featuredEl) {
        featuredEl.style.display = 'none';
    }

    // Recent articles
    const recentGrid = document.getElementById('recent-articles');
    if (recentGrid) {
        const recent = articles.slice(articles.length > 0 ? 1 : 0, 7);
        if (recent.length === 0) {
            recentGrid.innerHTML = `
                <div class="empty-state" style="grid-column:1/-1;">
                    <div class="empty-state-icon">&#128221;</div>
                    <p>Aun no hay articulos publicados.<br>Usa el <a href="admin.html">panel de administracion</a> para crear el primero.</p>
                </div>
            `;
        } else {
            recentGrid.innerHTML = '';
            recent.forEach(art => recentGrid.appendChild(createArticleCard(art)));
        }
    }
}

// ---- Articles Page ----
function initArticlesPage() {
    const grid = document.getElementById('articles-grid');
    const searchInput = document.getElementById('search-input');
    const tagFilter = document.getElementById('tag-filter');
    const dateFilter = document.getElementById('date-filter');

    if (!grid) return;

    function renderArticles() {
        const articles = getArticles().sort((a, b) => new Date(b.date) - new Date(a.date));
        const query = (searchInput?.value || '').toLowerCase().trim();
        const selectedTag = tagFilter?.value || '';
        const selectedDate = dateFilter?.value || '';

        let filtered = articles;

        if (query) {
            filtered = filtered.filter(a =>
                (a.title || '').toLowerCase().includes(query) ||
                (a.summary || '').toLowerCase().includes(query) ||
                (a.content || '').toLowerCase().includes(query) ||
                (a.tags || []).some(t => t.toLowerCase().includes(query))
            );
        }

        if (selectedTag) {
            filtered = filtered.filter(a => (a.tags || []).includes(selectedTag));
        }

        if (selectedDate) {
            const now = new Date();
            filtered = filtered.filter(a => {
                const ad = new Date(a.date);
                if (selectedDate === 'week') {
                    const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
                    return ad >= weekAgo;
                } else if (selectedDate === 'month') {
                    return ad.getMonth() === now.getMonth() && ad.getFullYear() === now.getFullYear();
                } else if (selectedDate === 'year') {
                    return ad.getFullYear() === now.getFullYear();
                }
                return true;
            });
        }

        grid.innerHTML = '';
        if (filtered.length === 0) {
            grid.innerHTML = '<div class="no-results" style="grid-column:1/-1;">No se encontraron articulos con esos criterios.</div>';
        } else {
            filtered.forEach(art => grid.appendChild(createArticleCard(art)));
        }
    }

    // Populate tag filter
    if (tagFilter) {
        const allTags = [...new Set(getArticles().flatMap(a => a.tags || []))].sort();
        allTags.forEach(tag => {
            const opt = document.createElement('option');
            opt.value = tag;
            opt.textContent = tag;
            tagFilter.appendChild(opt);
        });
    }

    searchInput?.addEventListener('input', renderArticles);
    tagFilter?.addEventListener('change', renderArticles);
    dateFilter?.addEventListener('change', renderArticles);

    renderArticles();
}

// ---- Single Article ----
function initArticlePage() {
    const params = new URLSearchParams(window.location.search);
    const id = params.get('id');
    const container = document.getElementById('article-container');

    if (!container) return;

    const article = getArticleById(id);

    if (!article) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">&#128533;</div>
                <h2>Articulo no encontrado</h2>
                <p>El articulo solicitado no existe o fue eliminado.</p>
                <a href="articulos.html" class="btn btn-primary mt-3">Ver todos los articulos</a>
            </div>
        `;
        return;
    }

    // Update page title and OG tags
    document.title = `${article.title} | Blog de Riesgos y Desastres`;
    updateMetaTag('og:title', article.title);
    updateMetaTag('og:description', article.summary || '');
    if (article.image) updateMetaTag('og:image', article.image);

    const shareUrl = encodeURIComponent(window.location.href);
    const shareTitle = encodeURIComponent(article.title);
    const linkedinShare = `https://www.linkedin.com/sharing/share-offsite/?url=${shareUrl}`;

    const tags = (article.tags || []).map(t => `<span class="tag">${escapeHtml(t)}</span>`).join('');

    // Convert content newlines to paragraphs
    const contentHtml = (article.content || '').split('\n').filter(p => p.trim()).map(p => `<p>${escapeHtml(p)}</p>`).join('');

    container.innerHTML = `
        <div class="article-single-header">
            <div class="article-card-tags mb-2">${tags}</div>
            <h1>${escapeHtml(article.title)}</h1>
            <div class="article-single-meta">
                <span>&#128197; Publicado: ${formatDate(article.date)}</span>
                ${article.updated ? `<span>&#128260; Actualizado: ${formatDate(article.updated)}</span>` : ''}
                <span class="version" style="background:rgba(27,153,139,0.2);color:#1B998B;padding:0.15rem 0.5rem;border-radius:4px;font-weight:600;">${escapeHtml(article.version || 'v1.0')}</span>
            </div>
            ${article.source ? `<p style="font-size:0.85rem;color:var(--color-text-muted);">Fuente: ${escapeHtml(article.source)}</p>` : ''}
        </div>

        ${article.image ? `<img src="${escapeHtml(article.image)}" alt="${escapeHtml(article.title)}" class="article-single-image">` : ''}

        <div class="article-content">
            ${contentHtml}
        </div>

        ${article.references ? `
        <div class="article-references">
            <h3>&#128218; Referencias</h3>
            ${article.references.split('\n').filter(r => r.trim()).map(r => `<p>${escapeHtml(r)}</p>`).join('')}
        </div>
        ` : ''}

        <div class="article-share-section">
            <p style="margin-bottom:1rem;color:var(--color-text-light);font-weight:600;">Comparte este articulo</p>
            <a href="${linkedinShare}" target="_blank" rel="noopener" class="btn btn-primary">&#x1F517; Compartir en LinkedIn</a>
        </div>
    `;
}

function updateMetaTag(property, content) {
    let meta = document.querySelector(`meta[property="${property}"]`);
    if (meta) {
        meta.setAttribute('content', content);
    }
}

// ---- Infographics lightbox ----
function initLightbox() {
    const lightbox = document.getElementById('lightbox');
    if (!lightbox) return;

    const img = lightbox.querySelector('img');
    const cards = document.querySelectorAll('.infographic-card');
    let currentIndex = 0;
    const images = [];

    cards.forEach((card, i) => {
        const cardImg = card.querySelector('img');
        if (cardImg) {
            images.push(cardImg.src);
            card.addEventListener('click', () => {
                currentIndex = i;
                img.src = images[currentIndex];
                lightbox.classList.add('active');
                document.body.style.overflow = 'hidden';
            });
        }
    });

    lightbox.querySelector('.lightbox-close')?.addEventListener('click', closeLightbox);
    lightbox.addEventListener('click', (e) => {
        if (e.target === lightbox) closeLightbox();
    });

    lightbox.querySelector('.lightbox-prev')?.addEventListener('click', (e) => {
        e.stopPropagation();
        currentIndex = (currentIndex - 1 + images.length) % images.length;
        img.src = images[currentIndex];
    });

    lightbox.querySelector('.lightbox-next')?.addEventListener('click', (e) => {
        e.stopPropagation();
        currentIndex = (currentIndex + 1) % images.length;
        img.src = images[currentIndex];
    });

    document.addEventListener('keydown', (e) => {
        if (!lightbox.classList.contains('active')) return;
        if (e.key === 'Escape') closeLightbox();
        if (e.key === 'ArrowLeft') {
            currentIndex = (currentIndex - 1 + images.length) % images.length;
            img.src = images[currentIndex];
        }
        if (e.key === 'ArrowRight') {
            currentIndex = (currentIndex + 1) % images.length;
            img.src = images[currentIndex];
        }
    });

    function closeLightbox() {
        lightbox.classList.remove('active');
        document.body.style.overflow = '';
    }
}

// ---- Contact form ----
function initContactForm() {
    const form = document.getElementById('contact-form');
    if (!form) return;

    form.addEventListener('submit', (e) => {
        e.preventDefault();
        const data = new FormData(form);
        const name = data.get('name');
        const email = data.get('email');
        const subject = data.get('subject');
        const message = data.get('message');

        // Create mailto link
        const mailtoUrl = `mailto:vpintopaez@hotmail.com?subject=${encodeURIComponent(subject || 'Contacto desde el blog')}&body=${encodeURIComponent(`Nombre: ${name}\nEmail: ${email}\n\n${message}`)}`;
        window.location.href = mailtoUrl;

        showToast('Abriendo cliente de correo...');
        form.reset();
    });
}

// ---- RSS feed generation ----
function generateRSS() {
    const articles = getArticles().sort((a, b) => new Date(b.date) - new Date(a.date));
    const baseUrl = window.location.origin;

    let rss = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
<channel>
    <title>Blog de Riesgos y Desastres - Victor Hugo Pinto Paez</title>
    <link>${baseUrl}</link>
    <description>Gestion de riesgos, desastres, geoinformacion y cambio climatico</description>
    <language>es</language>
    <lastBuildDate>${new Date().toUTCString()}</lastBuildDate>
    <atom:link href="${baseUrl}/rss.xml" rel="self" type="application/rss+xml"/>
`;

    articles.slice(0, 20).forEach(art => {
        rss += `    <item>
        <title>${escapeXml(art.title)}</title>
        <link>${baseUrl}/articulo.html?id=${art.id}</link>
        <guid>${baseUrl}/articulo.html?id=${art.id}</guid>
        <pubDate>${new Date(art.date + 'T00:00:00').toUTCString()}</pubDate>
        <description>${escapeXml(art.summary || '')}</description>
    </item>\n`;
    });

    rss += `</channel>\n</rss>`;
    return rss;
}

function escapeXml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function downloadRSS() {
    const rss = generateRSS();
    const blob = new Blob([rss], { type: 'application/rss+xml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'rss.xml';
    a.click();
    URL.revokeObjectURL(url);
}

// ---- Init ----
document.addEventListener('DOMContentLoaded', () => {
    initMobileMenu();

    const page = document.body.dataset.page;
    switch (page) {
        case 'home': initHomePage(); break;
        case 'articles': initArticlesPage(); break;
        case 'article': initArticlePage(); break;
        case 'infographics': initLightbox(); break;
        case 'contact': initContactForm(); break;
    }
});
