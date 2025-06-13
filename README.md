# Présentation (Fr)

**Dans le cadre de ses travaux sur les cookies et autres traceurs, le LINC publie deux études permettant de visualiser les interactions entre les différents acteurs de la publicité en ligne. Se basant sur un protocole de "scraping", ces études permettent une compréhension objective des mécanismes implémentés et de l'ampleur de leur utilisation.**

Pour permettre une plus grande confiance entre les acteurs de la publicité en ligne, l'IAB (Interactive Advertisement Bureau) propose à ceux-ci de déclarer leurs relations professionnelles via l'utilisation de deux standards :
* **Le standard Ads.txt** permettant aux éditeurs de sites web de déclarer les sociétés autorisées à vendre l'espace publicitaire dont ils disposent
* **Le standard Sellers.json** permettant aux vendeurs d'espaces publicitaires (SSP pour supply-side platform), de lister leurs fournisseurs d'espaces publicitaires, que ceux-ci soient des éditeurs ou des intermédiaires.

Ce repo contient les codes des pages web de ces études, ainsi que les outils ayant permis de collecter les données et générer les visualisations:
* Les codes sources des articles sont disponible [en anglais](./Articles/En/) et [en francais](./Articles/Fr/).
* Les codes sources sont disponibles [ici](./Sources). Pour plus d'information, lisez les README de [Ads.txt](./Sources/AdsTxt/README.md) et [Sellers.json](./Sources/SellersJson/README.md)

Enfin, si vous souhaitez lire les articles, ils sont disponibles directement sur le LINC:
* [L'article sur Ads.txt](https://linc.cnil.fr/webpub-adstxt-sellersjson/ads_study.html)
* [L'article sur Sellers.json](https://linc.cnil.fr/webpub-adstxt-sellersjson/sellers_study.html)

## Données personnelles

Cette étude repose sur la collecte d'URL et de données exclusivement relatives à des personnes morales. Cependant, dans certains cas, ces informations sont susceptibles de  comprendre des données à caractère personnel. Ce traitement est mis en œuvre par la [CNIL](http://www.cnil.fr/). Il est fondé sur l'exercice de l'autorité publique et a pour finalité la production d'études sur les usages des technologies. Les données collectées sont relatives aux noms de domaines des sites web publiquement accessibles via Internet. Ces données seront conservées pendant une durée maximale de 5 ans. Pour en savoir plus sur les modalités de gestion de vos données ou exercer vos droits, vous pouvez [consulter cette page](https://www.cnil.fr/fr/donnees-personnelles).

Si vous utilisez le code source fourni pour collecter des données vous-même, vous êtes également susceptible de collecter et de traiter des données à caractère personnel. Dans ce cas, vous devez vous assurer du complet respect de vos obligations prévues par le RGPD, notamment en termes d'information des personnes concernées et de respect de leurs autres droits définis par le règlement européen.

Toute réutilisation de données publiées qui auraient la nature de données personnelles suppose préalablement, de la part du réutilisateur, la vérification du complet respect de ses obligations prévues par le RGPD, notamment en termes d'information des personnes concernées et de respect de leurs autres droits définis par le règlement européen.

## Licence

La licence utilisée est la [licence ouverte 2.0](./LICENSE.md). Certaines parties du projet sont soumis à une licence legacy BSD 2-clause compatible avec celle-ci.


# Presentation (En)

**As part of its work on cookies and other tracers, the LINC is publishing two studies to visualize the interactions between the various players in online advertising. Based on a 'scraping' protocol, these studies provide an objective understanding of the mechanisms implemented and the extent of their use.**

In order to allow greater trust between online advertising professionals, the IAB (Interactive Advertisement Bureau) proposes that they declare their business relationships through the use of two standards:
* **The Ads.txt standard** allowing website publishers to declare which companies are authorised to sell the advertising space they have available.
* **The Sellers.json standard** allows advertising space sellers (SSP for supply-side platform) to list their advertising inventory suppliers, whether they are publishers or intermediaries.  

This repo contains the source code of the webpages of the studies, as well as the tools used to collect the data and generate the visualizations.
* The source code of the webpages is available [in English](./Articles/En/) and [in French](./Articles/Fr/).
* The source code is available [here](./Sources). For more information, read the README of [Ads.txt](./Sources/AdsTxt/README.md) and [Sellers.json](./Sources/SellersJson/README.md)

And if you just whish to read the articles, they are available on the LINC website:
* [The article on Ads.txt](https://linc.cnil.fr/webpub-adstxt-sellersjson/en/ads_study.html)
* [The article on Sellers.json](https://linc.cnil.fr/webpub-adstxt-sellersjson/en/sellers_study.html)

## Personal data

This study is based on the collection of URL and data that is exclusively related to legal persons. However, in some instance this data might contain personal data. This processing is carried out by the [CNIL](http://www.cnil.fr/). It is based on the exercise of an official authority and its purpose is to produce studies on the use of technology. The data collected is related to website domain names that are freely accessible over the Internet. This data will be stored for a maximum duration of 5 years.  For more information on the way the data is processed or to exercise your rights, you can [consult this page](https://www.cnil.fr/fr/donnees-personnelles).  

If you use the source code provided to collect data yourself, you might be collecting and processing personal data. In that case, you should ensure that you fully comply with any obligation stemming from the GDPR, in particular regarding the information of the data subject and the respect of their rights as laid out in the European regulation.

Any reuse of the published data, if said data were personal data, implies that the reuser should beforehand check that he fully complies with any obligation stemming from the GDPR, in particular regarding the information of the data subject and the respect of their rights  as laid out in the European regulation.

## License

The license used is the [open license 2.0](./LICENSE.md). Some parts of the project are subjected to a legacy BSD 2-clause license, compatible with the open license 2.0.

# AdTech Scraper

A Python-based tool for scraping and analyzing ads.txt and sellers.json files from publisher domains. This tool helps SSPs and publishers collect and process data about authorized digital sellers and their relationships.

## Features

- Asynchronous scraping of ads.txt and sellers.json files
- Support for multiple domains
- Structured data parsing
- CSV output for easy analysis
- Error handling and logging
- Support for both standard and .well-known paths for sellers.json

## Requirements

- Python 3.7+
- Required packages (install using `pip install -r requirements.txt`):
  - requests
  - beautifulsoup4
  - pandas
  - tqdm
  - python-dotenv
  - aiohttp
  - asyncio

## Installation

1. Clone this repository
2. Install the required packages:
```bash
pip install -r requirements.txt
```

## Usage

1. Modify the `domains` list in `scraper.py` with the domains you want to analyze:
```python
domains = [
    'example.com',
    'publisher1.com',
    'publisher2.com'
]
```

2. Run the scraper:
```bash
python scraper.py
```

3. The results will be saved in the `output` directory:
   - `ads_txt_results.csv`: Contains parsed ads.txt data
   - `sellers_json_results.csv`: Contains parsed sellers.json data

## Output Format

### ads.txt Results
- domain: The publisher's domain
- ad_system_domain: The advertising system domain
- publisher_id: The publisher's account ID
- account_type: The type of account (DIRECT, RESELLER)
- certification_authority_id: The certification authority ID (if present)

### sellers.json Results
- domain: The publisher's domain
- seller_id: The seller's ID
- name: The seller's name
- domain: The seller's domain
- seller_type: The type of seller
- is_confidential: Whether the seller is confidential
- is_passthrough: Whether the seller is a passthrough

## Error Handling

The scraper includes comprehensive error handling:
- Failed requests are logged but don't stop the process
- Invalid JSON in sellers.json is handled gracefully
- Network timeouts are managed
- Results are saved even if some domains fail

## Contributing

Feel free to submit issues and enhancement requests!

# Scraper Ads.txt et Sellers.json

Ce script permet de scraper les fichiers `ads.txt` et `sellers.json` des sites web pour analyser les relations entre les différents acteurs de la publicité programmatique.

## Fonctionnalités

- Scraping des fichiers `ads.txt` et `sellers.json`
- Analyse des relations entre éditeurs et intermédiaires
- Génération de rapports hebdomadaires sur les nouveaux domaines
- Export des données vers Google Sheets

## Prérequis

- Python 3.7+
- Bibliothèques requises (voir `requirements.txt`)

## Installation

1. Cloner le dépôt :
```bash
git clone [URL_DU_REPO]
```

2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

3. Configurer les identifiants Google Sheets dans le script

## Utilisation

```bash
python ssp_scraper.py
```

## Structure du projet

- `ssp_scraper.py` : Script principal
- `List of SSP.csv` : Liste des SSP à analyser
- `output/` : Dossier contenant les résultats
- `last_week_domains.json` : Suivi des domaines par semaine

## Licence

[À définir]
