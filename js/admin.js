/* ========================================
   Blog Riesgos y Desastres - Admin Panel JS
   ======================================== */

const STORAGE_KEY = 'blog_riesgos_articles';

function getArticles() {
    try {
        return JSON.parse(localStorage.getItem(STORAGE_KEY)) || [];
    } catch {
        return [];
    }
}

function saveArticles(articles) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(articles));
}

function generateId() {
    return Date.now().toString(36) + Math.random().toString(36).substr(2, 5);
}

// State
let editingId = null;

// DOM elements
const form = document.getElementById('article-form');
const titleInput = document.getElementById('art-title');
const summaryInput = document.getElementById('art-summary');
const contentInput = document.getElementById('art-content');
const imageInput = document.getElementById('art-image');
const tagsInput = document.getElementById('art-tags');
const dateInput = document.getElementById('art-date');
const versionInput = document.getElementById('art-version');
const sourceInput = document.getElementById('art-source');
const referencesInput = document.getElementById('art-references');
const listContainer = document.getElementById('articles-list');
const formTitle = document.getElementById('form-title');
const cancelBtn = document.getElementById('cancel-edit');

function init() {
    // Set default date to today
    if (dateInput && !dateInput.value) {
        dateInput.value = new Date().toISOString().split('T')[0];
    }

    form?.addEventListener('submit', handleSubmit);
    cancelBtn?.addEventListener('click', resetForm);

    renderList();
    initMobileMenu();
}

function handleSubmit(e) {
    e.preventDefault();

    // Validate required fields
    if (!titleInput.value.trim()) {
        showToast('El titulo es obligatorio');
        titleInput.focus();
        return;
    }

    if (!sourceInput.value.trim()) {
        showToast('La fuente es obligatoria (rigor academico)');
        sourceInput.focus();
        return;
    }

    if (!referencesInput.value.trim()) {
        showToast('Las referencias bibliograficas son obligatorias');
        referencesInput.focus();
        return;
    }

    const articles = getArticles();

    const articleData = {
        title: titleInput.value.trim(),
        summary: summaryInput.value.trim(),
        content: contentInput.value.trim(),
        image: imageInput.value.trim(),
        tags: tagsInput.value.split(',').map(t => t.trim()).filter(Boolean),
        date: dateInput.value,
        version: versionInput.value.trim() || 'v1.0',
        source: sourceInput.value.trim(),
        references: referencesInput.value.trim()
    };

    if (editingId) {
        // Update existing
        const index = articles.findIndex(a => a.id === editingId);
        if (index !== -1) {
            articleData.id = editingId;
            articleData.updated = new Date().toISOString().split('T')[0];
            // Preserve original date
            articleData.date = articles[index].date;
            // Auto-increment version
            const oldVersion = articles[index].version || 'v1.0';
            if (articleData.version === oldVersion) {
                const parts = oldVersion.replace('v', '').split('.');
                const minor = parseInt(parts[1] || 0) + 1;
                articleData.version = `v${parts[0]}.${minor}`;
            }
            articles[index] = articleData;
        }
        showToast('Articulo actualizado correctamente');
    } else {
        // Create new
        articleData.id = generateId();
        articles.push(articleData);
        showToast('Articulo publicado correctamente');
    }

    saveArticles(articles);
    resetForm();
    renderList();
}

function editArticle(id) {
    const article = getArticles().find(a => a.id === id);
    if (!article) return;

    editingId = id;
    titleInput.value = article.title || '';
    summaryInput.value = article.summary || '';
    contentInput.value = article.content || '';
    imageInput.value = article.image || '';
    tagsInput.value = (article.tags || []).join(', ');
    dateInput.value = article.date || '';
    versionInput.value = article.version || 'v1.0';
    sourceInput.value = article.source || '';
    referencesInput.value = article.references || '';

    formTitle.textContent = 'Editar Articulo';
    cancelBtn.style.display = 'inline-flex';

    window.scrollTo({ top: 0, behavior: 'smooth' });
}

function deleteArticle(id) {
    if (!confirm('¿Estas seguro de eliminar este articulo?')) return;

    const articles = getArticles().filter(a => a.id !== id);
    saveArticles(articles);
    renderList();
    showToast('Articulo eliminado');

    if (editingId === id) resetForm();
}

function resetForm() {
    editingId = null;
    form?.reset();
    if (dateInput) dateInput.value = new Date().toISOString().split('T')[0];
    if (versionInput) versionInput.value = 'v1.0';
    if (formTitle) formTitle.textContent = 'Nuevo Articulo';
    if (cancelBtn) cancelBtn.style.display = 'none';
}

function renderList() {
    if (!listContainer) return;

    const articles = getArticles().sort((a, b) => new Date(b.date) - new Date(a.date));

    if (articles.length === 0) {
        listContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">&#128221;</div>
                <p>No hay articulos publicados aun.</p>
            </div>
        `;
        return;
    }

    listContainer.innerHTML = articles.map(art => `
        <div class="admin-article-item">
            <div>
                <h4>${escapeHtml(art.title)}</h4>
                <span class="meta">
                    ${formatDate(art.date)} | ${art.version || 'v1.0'} |
                    Etiquetas: ${(art.tags || []).join(', ') || 'ninguna'}
                    ${art.updated ? ' | Actualizado: ' + formatDate(art.updated) : ''}
                </span>
            </div>
            <div class="admin-article-actions">
                <button class="btn-sm btn-edit" onclick="editArticle('${art.id}')">Editar</button>
                <button class="btn-sm btn-danger" onclick="deleteArticle('${art.id}')">Eliminar</button>
            </div>
        </div>
    `).join('');
}

function formatDate(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr + 'T00:00:00');
    const months = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic'];
    return `${d.getDate()} ${months[d.getMonth()]} ${d.getFullYear()}`;
}

function escapeHtml(str) {
    if (!str) return '';
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

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

function initMobileMenu() {
    const toggle = document.querySelector('.menu-toggle');
    const nav = document.querySelector('.nav-links');
    if (toggle && nav) {
        toggle.addEventListener('click', () => {
            nav.classList.toggle('open');
        });
    }
}

// Export articles as JSON backup
function exportArticles() {
    const articles = getArticles();
    const blob = new Blob([JSON.stringify(articles, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'blog-articulos-backup.json';
    a.click();
    URL.revokeObjectURL(url);
    showToast('Backup descargado');
}

// Import articles from JSON
function importArticles(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = function(e) {
        try {
            const imported = JSON.parse(e.target.result);
            if (Array.isArray(imported)) {
                const existing = getArticles();
                const existingIds = new Set(existing.map(a => a.id));
                const newArticles = imported.filter(a => !existingIds.has(a.id));
                saveArticles([...existing, ...newArticles]);
                renderList();
                showToast(`Se importaron ${newArticles.length} articulos nuevos`);
            }
        } catch {
            showToast('Error al importar el archivo');
        }
    };
    reader.readAsText(file);
}

document.addEventListener('DOMContentLoaded', init);
