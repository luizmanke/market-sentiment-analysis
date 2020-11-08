# System packages
import json
import os
from datetime import datetime, timedelta
from google.cloud import tasks_v2
from google.oauth2.service_account import Credentials


def run(request):
    COMPANIES = {
        "ABEV": {"id": "", "searches": ["ambev", "abev", "abev3"]},
        "AZUL": {"id": "", "searches": ["azul linhas aereas", "azul", "azul4"]},
        "B3SA": {"id": "", "searches": ["b3", "b3sa", "b3sa3"]},
        "BBAS": {"id": "", "searches": ["banco do brasil", "bbas", "bbas3"]},
        "BBDC": {"id": "", "searches": ["bradesco", "bbdc", "bbdc3", "bbdc4"]},
        "BBSE": {"id": "", "searches": ["bbseguridade", "bbse", "bbse3"]},
        "BEEF": {"id": "", "searches": ["minerva", "beef", "beef3"]},
        "BPAC": {"id": "", "searches": ["banco pactual", "btg", "bpac", "bpac11"]},
        "BRAP": {"id": "", "searches": ["bradespar", "brap", "brap4"]},
        "BRDT": {"id": "", "searches": ["petrobras distribuitora", "brdt", "brdt3"]},
        "BRFS": {"id": "", "searches": ["brasil foods", "brf", "brfs", "brfs3"]},
        "BRKM": {"id": "", "searches": ["braskem", "brkm", "brkm5"]},
        "BRML": {"id": "", "searches": ["br malls", "brml", "brml3"]},
        "BTWO": {"id": "", "searches": ["b2w digital", "btwo", "btwo3"]},
        "CCRO": {"id": "", "searches": ["ccr", "ccro", "ccro3"]},
        "CIEL": {"id": "", "searches": ["cielo", "ciel", "ciel3"]},
        "CMIG": {"id": "", "searches": ["cemig", "cmig", "cmig4"]},
        "COGN": {"id": "", "searches": ["cogna", "cogn", "cogn3"]},
        "CPFE": {"id": "", "searches": ["cpfl", "cpfe", "cpfe3"]},
        "CRFB": {"id": "", "searches": ["carrefour", "crfb", "crfb3"]},
        "CSAN": {"id": "", "searches": ["cosan", "csan", "csan3"]},
        "CSNA": {"id": "", "searches": ["siderurgica nacional", "csna", "csna3"]},
        "CVCB": {"id": "", "searches": ["cvc", "cvcb", "cvcb3"]},
        "CYRE": {"id": "", "searches": ["cyrela", "cyre", "cyre3"]},
        "ECOR": {"id": "", "searches": ["ecorodovias", "ecor", "ecor3"]},
        "EGIE": {"id": "", "searches": ["engie", "egie", "egie3"]},
        "ELET": {"id": "", "searches": ["eletrobras", "elet", "elet3", "elet6"]},
        "EMBR": {"id": "", "searches": ["embraer", "embr", "embr3"]},
        "ENBR": {"id": "", "searches": ["energias do brasil", "enbr", "enbr3"]},
        "ENGI": {"id": "", "searches": ["energisa", "engi", "engi11"]},
        "EQTL": {"id": "", "searches": ["equatorial", "eqtl", "eqtl3"]},
        "EZTC": {"id": "", "searches": ["eztec", "eztc", "eztc3"]},
        "FLRY": {"id": "", "searches": ["fleury", "flry", "flry3"]},
        "GGBR": {"id": "", "searches": ["gerdau", "ggbr", "ggbr4"]},
        "GNDI": {"id": "", "searches": ["intermedica", "gndi", "gndi3"]},
        "GOAU": {"id": "", "searches": ["metalurgica gerdau", "goau", "goau4"]},
        "GOLL": {"id": "", "searches": ["gol linhas aereas", "goll", "goll4"]},
        "HAPV": {"id": "", "searches": ["hapvida", "hapv", "hapv3"]},
        "HGTX": {"id": "", "searches": ["hering", "hgtx", "hgtx3"]},
        "HYPE": {"id": "", "searches": ["hypera", "hype", "hype3"]},
        "IGTA": {"id": "", "searches": ["iguatemi", "igta", "igta3"]},
        "IRBR": {"id": "", "searches": ["irb", "irbr", "irbr3"]},
        "ITSA": {"id": "", "searches": ["itausa", "itsa", "itsa3", "itsa4"]},
        "ITUB": {"id": "", "searches": ["itau", "itub", "itub3", "itub4"]},
        "JBSS": {"id": "", "searches": ["jbs", "jbss", "jbss3"]},
        "KLBN": {"id": "", "searches": ["klabin", "klbn", "klbn11"]},
        "LAME": {"id": "", "searches": ["lojas americanas", "lame", "lame4"]},
        "LREN": {"id": "", "searches": ["renner", "lren", "lren3"]},
        "MGLU": {"id": "", "searches": ["magazine luiza", "mglu", "mglu3"]},
        "MRFG": {"id": "", "searches": ["marfrig", "mrfg", "mrfg3"]},
        "MRVE": {"id": "", "searches": ["mrv", "mrve", "mrve3"]},
        "MULT": {"id": "", "searches": ["multiplan", "mult", "mult3"]},
        "NTCO": {"id": "", "searches": ["natura", "ntco", "ntco3"]},
        "PCAR": {"id": "", "searches": ["pao de açucar", "pcar", "pcar3"]},
        "PETR": {"id": "", "searches": ["petrobras", "petr", "petr3", "petr4"]},
        "PRIO": {"id": "", "searches": ["petrorio", "prio", "prio3"]},
        "QUAL": {"id": "", "searches": ["qualicorp", "qual", "qual3"]},
        "RADL": {"id": "", "searches": ["raiadrogasil", "radl", "radl3"]},
        "RAIL": {"id": "", "searches": ["rumo s.a.", "rail", "rail3"]},
        "RENT": {"id": "", "searches": ["localiza", "rent", "rent3"]},
        "SANB": {"id": "", "searches": ["santander", "sanb", "sanb11"]},
        "SBSP": {"id": "", "searches": ["sabesp", "sbsp", "sbsp3"]},
        "SULA": {"id": "", "searches": ["sul america", "sula", "sula11"]},
        "SUZB": {"id": "", "searches": ["suzano", "suzb", "suzb3"]},
        "TAEE": {"id": "", "searches": ["taesa", "taee", "taee11"]},
        "TIMS": {"id": "", "searches": ["tim", "tims", "tims3"]},
        "TOTS": {"id": "", "searches": ["totvs", "tots", "tots3"]},
        "UGPA": {"id": "", "searches": ["ultrapar", "ugpa", "ugpa3"]},
        "USIM": {"id": "", "searches": ["usiminas", "usim", "usim5"]},
        "VALE": {"id": "", "searches": ["vale s.a.", "vale", "vale3"]},
        "VIVT": {"id": "", "searches": ["telefonica brasil", "vivt", "vivt4"]},
        "VVAR": {"id": "", "searches": ["viavarejo", "vvar", "vvar3"]},
        "WEGE": {"id": "", "searches": ["weg", "wege", "wege3"]},
        "YDUQ": {"id": "", "searches": ["yduqs", "yduq", "yduq3"]}
    }
    _run(COMPANIES)


def _run(companies):
    since, until = _get_period()
    _publish_tweet_requests(companies, since, until)


def _get_period():
    now = datetime.now()
    since = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    until = now.strftime("%Y-%m-%d")
    print(f" - since: {since}  until: {until}")
    return since, until


def _publish_tweet_requests(companies, since, until):
    PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    REGION = os.getenv("GOOGLE_REGION")
    SERVICE_ACCOUNT_EMAIL = os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL")
    url = f"https://{REGION}-{PROJECT_ID}.cloudfunctions.net/request_tweets"
    client, queue_path = _connect_to_google_queue()
    date = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    for ticker, values in companies.items():
        data = {
            "ticker": ticker,
            "searches": values["searches"],
            "since": since,
            "until": until
        }
        data = json.dumps(data)
        data = data.encode("utf-8")

        task = {
            "name": (
                f"projects/{PROJECT_ID}/locations/southamerica-east1/"
                f"queues/tweet-request-queue/tasks/{ticker}-{date}"
            ),
            "http_request": {
                "oidc_token": {"service_account_email": SERVICE_ACCOUNT_EMAIL},
                "http_method": tasks_v2.HttpMethod.POST,
                "headers": {"Content-type": "application/json"},
                "url": url,
                "body": data,
            }
        }
        response = client.create_task(request={"parent": queue_path, "task": task})

        print(
            f" - ticker: {ticker}  "
            f"searches: {values['searches']}  "
            f"task: {response.name}"
        )


def _connect_to_google_queue():
    CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
    PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
    credentials = Credentials.from_service_account_info(json.loads(CREDENTIALS))
    client = tasks_v2.CloudTasksClient(credentials=credentials)
    queue_path = client.queue_path(PROJECT_ID, "southamerica-east1", "tweet-request-queue")
    return client, queue_path
