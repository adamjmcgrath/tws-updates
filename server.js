const express = require('express');
const updates = require('./updates');

const app = express();

app.get('/', async (req, res) => {
  res.send('TWS Updates');
});

app.get('/updates', updates);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}...`);
});
