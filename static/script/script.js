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
    search.classList.add('loading');
    
    output.innerHTML = `
        <div class="loading-state">
            <div class="big-loader"></div>
            <h3>Ricerca in corso<span class="loading-dots"></span></h3>
            <p>Stiamo analizzando gli annunci su Meta Ads Library</p>
        </div>
    `;

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
        search.classList.remove('loading');

        if (data.error) {
            output.innerHTML = `<div class="empty-state">${data.error}</div>`;
        } else {
            // Clear container
            output.innerHTML = '';

            // Create cards
            data.message.forEach((lead, i) => {
                const card = document.createElement('div');
                card.className = 'lead-card';
                
                // Determina classe badge
                let badgeClass = '';
                if (lead.copy_valutazione && lead.copy_valutazione.includes('molto interessante')) {
                    badgeClass = 'very-interesting';
                } else if (lead.copy_valutazione && lead.copy_valutazione.includes('interessante')) {
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
                            <span class="copy-badge ${badgeClass}">${lead.copy_valutazione || 'N/A'}</span>
                        </div>
                    </div>
                `;
                output.appendChild(card);
            });
        }
    })
    .catch(err => {
        search.disabled = false;
        search.classList.remove('loading');
        output.innerHTML = `<div class="error-state">❌ Errore: ${err}</div>`;
    });
});

// Enter key support
query.addEventListener('keypress', function(e) {
    if (e.key === 'Enter' && !search.disabled) {
        search.click();
    }
});