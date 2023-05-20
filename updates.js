const fetch = require('node-fetch');
const neatCsv = require('neat-csv');
const slugify = require('slugify');
const { KVStore } = require('google-cloud-kvstore');
const { Datastore } = require('@google-cloud/datastore');
const sgMail = require('@sendgrid/mail');
const winston = require('winston');
const {LoggingWinston} = require('@google-cloud/logging-winston');

const store = new KVStore(new Datastore({ projectId: 'twsupdates' }));

const URL =
  'https://www.thewinesociety.com/CustomFileDownload/DownloadCsv?contentId=1073743634&parameters=%7B%22Body%22%3A%22%22%2C%22Closure%22%3A%22%22%2C%22Country%22%3A%22%22%2C%22DrinkEndFrom%22%3A%22%22%2C%22DrinkEndTo%22%3A%22%22%2C%22DrinkStartFrom%22%3A%22%22%2C%22DrinkStartTo%22%3A%22%22%2C%22Food%22%3A%22%22%2C%22Grape%22%3A%22%22%2C%22HideCountry%22%3Afalse%2C%22HideRegion%22%3Afalse%2C%22HideSubRegion%22%3Afalse%2C%22LevelMaximum%22%3Anull%2C%22LevelMinimum%22%3Anull%2C%22Oak%22%3A%22%22%2C%22Organic%22%3A%22%22%2C%22Producer%22%3A%22%22%2C%22ProductType%22%3A%22%22%2C%22Region%22%3A%22%22%2C%22Style%22%3A%22%22%2C%22SubRegion%22%3A%22%22%2C%22Unit%22%3A%22%22%2C%22VintageFrom%22%3A%22%22%2C%22VintageTo%22%3A%22%22%2C%22ReserveAction%22%3A1%2C%22DrinkStatus%22%3Anull%2C%22OnOfferStatus%22%3Anull%2C%22PartsListQuantity%22%3Anull%2C%22PriceMaximum%22%3Anull%2C%22PriceMinimum%22%3Anull%2C%22PriceRange%22%3A%22%22%2C%22Rating%22%3A%22%22%2C%22Saving%22%3A%22%22%2C%22Sort%22%3A7%2C%22StarRatingFrom%22%3Anull%2C%22StarRatingTo%22%3Anull%2C%22Status%22%3A%22%22%2C%22Type%22%3A1%2C%22View%22%3A1%2C%22TempCategoryContent%22%3A%221073742303__CatalogContent%22%2C%22Page%22%3A1%2C%22PageSize%22%3A90%2C%22q%22%3A%22%22%2C%22SearchTerm%22%3A%22%22%7D&rel=nofollow';

const CASE_OFFERS_RE = /^SC\d/;

const loggingWinston = new LoggingWinston();

const logger = winston.createLogger({
  level: 'info',
  transports: [
    new winston.transports.Console(),
    loggingWinston,
  ],
});

module.exports = async function (req, res) {
  const cron = req.headers['x-appengine-cron'];
  logger.info(`CRON: ${cron}`);
  if (!cron) {
    res.send(404);
    return;
  }

  const response = await fetch(URL);
  logger.info(`got url: ${URL}`);
  const data = (await response.text()).split('\n').slice(1).join('\n');
//   const data = `Product title,Vintage,Price,Origin,Product code,Drink date,Grape,Type,Alcohol,Style,Closure type,Description
// Undurraga Cauquenes Estate Maule Viognier Roussanne Marsanne 2022,2022,8.50 / Bottle,,CE12571,2023 - 2026,Viognier/Rousanne/Marsanne,White Wine,0%,2 - Dry,Screwcap,Undurraga Cauquenes Estate Maule Viognier Roussanne Marsanne 2022
// Rede Reserva Tinto Douro 2017,2017,9.50 / Bottle,,PW9921,2021 - 2024,,Red Wine,13.5%,Full-bodied,"Cork, natural",Rede Reserva Tinto Douro 2017
// Quinta da Pedra Alta Reserva Branco Douro 2020,2020,22.00 / Bottle,,PW10091,2022 - 2028,,White Wine,12.5%,2 - Dry,"Cork, natural",Quinta da Pedra Alta Reserva Branco Douro 2020
// "Pinot Noir, Puy de Dôme, Cave Saint-Verny 2021",2021,9.95 / Bottle,Provence / Massif / Corsica,FC45061,2023 - 2026,Pinot Noir,Red Wine,12.5%,Medium-bodied,Screwcap,"Lovely, ripe, cherry-like pinot noir from the Auvergne in France where, in ancient times, this grape was known as auvernat. Deeply coloured, unoaked, full-flavoured with the taste of red berry and cherry fruit."
// Per Se Iubileus Gualtallary Malbec 2019,2019,115.00 / Bottle,,AR4821,2023 - 2037,Malbec/Cot,Red Wine,14%,Full-bodied,"Cork, natural","This is exquisite, combining intense floral aromas and long, linear, tightly coiled palate. Bright and fresh with polished tannins that glide over the palate. The finesse is derived from a vineyard planted in 2013 at almost 1500m on limestone soil in Gualtallary, Argentina. Matured in used 225-litre barrels for 16 months.1380 bottles made."
// Per Se Inseparable Gualtallary Malbec 2019,2019,36.00 / Bottle,,AR4831,2023 - 2030,Malbec/Cot,Red Wine,14.5%,Full-bodied,"Cork, diam",Per Se Inseparable Gualtallary Malbec 2019
// Lowlands Heritage Marlborough Sauvignon Blanc 2022,2022,11.50 / Bottle,,NZ13901,2022 - 2024,Sauvignon Blanc,White Wine,0%,2 - Dry,Screwcap,"The Holdaway family of New Zealand's Marlborough Valley has excelled once again with a wine which shows zesty citrus and tropical fruit on the nose and a crisp, dry and refreshing palate. Perfect for lovers of grassy, lemon-driven whites."
// Half bottle of The Society's Sicilian Reserve Red 2019,2019,5.25 / Bottle,S Italy and Islands,IT36902,2023 - 2025,Nero d'Avola,Red Wine,13.5%,Medium-bodied,"Cork, natural",Half bottle of The Society's Sicilian Reserve Red 2019
// "Fiano, Miopasso Sicilia 2021",2021,7.75 / Bottle (out of stock),S Italy and Islands,IT35571,2021 - 2023,Fiano,White Wine,13%,2 - Dry,Screwcap,"2021 in Sicily was a lovely vintage for characterful Fiano. With plenty of fresh apple and peach fruit on the nose, this dry, crisp white is versatile and refreshing."
// "Domaine des Tourelles Rouge, Lebanon 2020",2020,12.95 / Bottle,,LE1261,2022 - 2026,Cabernet Shiraz,Red Wine,14%,Full-bodied,"Cork, natural","A full-bodied Lebanese red from mostly syrah and cabernet grown in Lebanon's Bekaa Valley. Rich, peppery and velvety, this shows the emerging talent of the team at Tourelles."
// Chianti Classico Isole e Olena 2020,2020,24.00 / Bottle,"Central Italy -Tuscany, Umbria",IT37421,2023 - 2030,Sangiovese,Red Wine,14%,Full-bodied,"Cork, natural",Chianti Classico Isole e Olena 2020
// Chateau de Beauregard Fleurie Poncie 2021,2021,16.00 / Bottle,Beaujolais,BJ9631,2022 - 2025,Gamay,Red Wine,13%,Medium-bodied,"Cork, natural",Chateau de Beauregard Fleurie Poncie 2021
// Big Reds Case,0,52.00 / Case of 6,,SC23208A,N/A,,,0%,,,Big Reds Case`;

  const csv = await neatCsv(data);
  const cursor = await store.get('latest');
  logger.info(`got cursor: ${cursor}`);

  let latest;
  let out = [];

  // Product title	Vintage	Price	Origin	Product code	Drink date	Grape	Type	Alcohol	Style	Closure type	Description
  for (const row of csv) {
    const { 'Product code': id, 'Product title': title, Price: price, Description: desc, Type: type } = row;

    latest = latest || id;

    // Ignore wine case offers.
    if (CASE_OFFERS_RE.test(id)) {
      continue;
    }

    if (id == cursor) {
      break;
    }

    logger.info(`process ${id}`);

    out.push({
      url: `https://www.thewinesociety.com/product/${encodeURIComponent(
        slugify(row['Product title'].replace(/'/g, '')).toLowerCase()
      )}`,
      id,
      title,
      price,
      desc,
      type,
    });
  }

  if (!out.length) {
    logger.info('no updates');
    res.send('empty');
    return;
  }

  await store.set('latest', latest || cursor);
  logger.info(`Saved cursor ${latest || cursor}`);
  let type;

  const html = out.sort((a, b) => a.type > b.type ? 1 : -1).reduce((str, row) => {
    if (row.type !== type) {
      str += `<strong style="font-size: x-large;">${row.type}</strong><br><br>`;
      type = row.type;
    }
    return str + `<strong>${row.title}</strong><br>
${row.desc}<br>
<a href="${row.url}">${row.url}</a><br>
£${row.price}<br><br>`;}, '')

  const date = new Date();

  const to = 'adamjmcgrath@gmail.com';
  logger.info(`Sending mail to: ${'adamjmcgrath@gmail.com'}`);
  sgMail.setApiKey(await store.get('SENDGRID_API_KEY'));
  await sgMail.send({
    to,
    from: 'adamjmcgrath@gmail.com',
    subject: 'TWS Update: ' + `${date.toLocaleDateString()} ${date.toLocaleTimeString()}`,
    html,
  });
  logger.info(`Sent mail`);

  res.send(html);
};
