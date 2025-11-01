let search = document.getElementById("search");
let output = document.getElementById("leads_container");
let query = document.getElementById("query");

search.addEventListener("click", function() {
    if (!query.value.trim()) {
        alert('Inserisci una query valida');
        return;
    }

    // Loading state
    search.disabled = true;
    search.textContent = 'Ricerca in corso...';
    output.innerHTML = '<div style="text-align: center; padding: 40px; color: #ff6666;">🔍 Ricerca leads in corso...</div>';

    fetch("/add_leads", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({ "query": query.value })
    })
    .then(response => response.json())
    .then(data => {
        // Reset button
        search.disabled = false;
        search.textContent = 'Research lead';

        if (data.error) {
            output.innerHTML = `<div style="text-align: center; padding: 40px; color: rgba(255, 255, 255, 0.5);">${data.error}</div>`;
        } else {
            // Clear container
            output.innerHTML = '';

            // Create cards
            data.message.forEach((lead, i) => {
                const card = document.createElement('div');
                card.className = 'lead-card';
                
                // Determina classe badge
                let badgeClass = '';
                if (lead.copy_valutazione === 'Copy molto interessante') {
                    badgeClass = 'very-interesting';
                } else if (lead.copy_valutazione === 'Copy interessante') {
                    badgeClass = 'interesting';
                }
                
                // Check contatti
                const hasEmail = lead.email && lead.email !== 'Non trovata';
                const hasPhone = lead.telefono && lead.telefono !== 'Non trovato';
                
                card.innerHTML = `
                    <h3>Lead #${i + 1}</h3>
                    <div class="lead-info">
                        <div class="lead-info-item">
                            <strong>📢 Link Ads</strong>
                            ${lead.ad_link && lead.ad_link !== 'Non disponibile' 
                                ? `<a href="${lead.ad_link}" target="_blank" class="ad-link">${lead.ad_link}</a>`
                                : '<span class="not-found">Non disponibile</span>'
                            }
                        </div>
                        <div class="lead-info-item">
                            <strong>🌐 Landing page</strong>
                            <a href="${lead.landing_page}" target="_blank">${lead.landing_page}</a>
                        </div>
                        <div class="lead-info-item">
                            <strong>📧 Email</strong>
                            <span class="${hasEmail ? '' : 'not-found'}">${lead.email || 'Non trovata'}</span>
                        </div>
                        <div class="lead-info-item">
                            <strong>📞 Telefono</strong>
                            <span class="${hasPhone ? '' : 'not-found'}">${lead.telefono || 'Non trovato'}</span>
                        </div>
                        <div class="lead-info-item">
                            <strong>✍️ Valutazione copy</strong>
                            <span class="copy-badge ${badgeClass}">${lead.copy_valutazione}</span>
                        </div>
                    </div>
                    <div class="lead-status-bar">
                        <div class="status-indicator ${hasEmail ? 'active' : 'inactive'}">
                            <span class="status-dot"></span>
                            <span>Email</span>
                        </div>
                        <div class="status-indicator ${hasPhone ? 'active' : 'inactive'}">
                            <span class="status-dot"></span>
                            <span>Telefono</span>
                        </div>
                    </div>
                `;
                output.appendChild(card);
            });
        }
    })
    .catch(err => {
        search.disabled = false;
        search.textContent = 'Research lead';
        output.innerHTML = `<div style="text-align: center; padding: 40px; color: #ff3333;">❌ Errore: ${err}</div>`;
    });
});