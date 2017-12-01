from flask import Flask, request, render_template, send_from_directory
from search_engine_mock import SearchEngine

app = Flask(__name__, template_folder='server_resources/templates')
searchEngine = SearchEngine()


@app.route("/", methods=['GET', 'POST'])
def main():
    return render_template('main.html')


@app.route("/ask", methods=['GET'])
def ask():
    query = request.args.get('query')
    results = searchEngine.ask(query)
    return render_template('results.html', results=results)


@app.route('/style.css')
def get_style():
    print('css updated!')
    return send_from_directory('server_resources/stylesheets', 'main.css')


@app.route('/main.js')
def get_js():
    print('js updated!')
    return send_from_directory('server_resources/scripts', 'main.js')


if __name__ == '__main__':
    app.run()
