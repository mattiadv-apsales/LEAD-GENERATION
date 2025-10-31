let search = document.getElementById("search");
let output = document.getElementById("leads_container");
let query = document.getElementById("query");

search.addEventListener("click", function() {
    output.innerHTML = "Sto cercando... ⏳";

    fetch("/add_leads", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({ "query": query.value })
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            output.innerHTML = `<p style="color:red">${data.error}</p>`;
        } else {
            let html = "<ul>";
            data.message.forEach(lead => {
                html += `<li>
                    <strong>Landing page:</strong> <a href="${lead.landing_page}" target="_blank">${lead.landing_page}</a><br>
                    <strong>Email:</strong> ${lead.email || "N/D"}<br>
                    <strong>Telefono:</strong> ${lead.telefono || "N/D"}<br>
                    <strong>Valutazione copy:</strong> ${lead.copy_valutazione}
                </li><br>`;
            });
            html += "</ul>";
            output.innerHTML = html;
        }
    })
    .catch(err => {
        output.innerHTML = `<p style="color:red">Errore: ${err}</p>`;
    });
});
