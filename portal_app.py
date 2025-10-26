from flask import Flask, render_template

# Opprett Flask-appen
app = Flask(__name__)

@app.route('/')
def landing():
    """Denne funksjonen kjører når noen besøker forsiden."""
    # Bare vis HTML-siden
    return render_template('landing_page.html')

if __name__ == '__main__':
    # Denne linjen er kun for lokal testing
    app.run(debug=True, port=5001) # Bruker en annen port for å unngå kollisjon