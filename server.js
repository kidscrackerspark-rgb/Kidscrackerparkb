const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();

app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

const storage = multer.diskStorage({
  destination: './uploads/',
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  },
});
const upload = multer({
  storage,
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const filetypes = /jpeg|jpg|png/;
    const extname = filetypes.test(path.extname(file.originalname).toLowerCase());
    const mimetype = filetypes.test(file.mimetype);
    if (extname && mimetype) {
      return cb(null, true);
    }
    cb(new Error('Only JPEG/PNG images are allowed'));
  },
});
app.use('/Uploads', express.static(path.join(__dirname, 'Uploads')));

app.use('/api', require('./Router/Inventory.router'));
app.use('/api', require('./Router/Admin.router'));
app.use('/api/locations', require('./Router/Location.router'));
app.use('/api/directcust', require('./Router/Directcust.router'));
app.use('/api/direct', require('./Router/Direct.router'));
app.use('/api/tracking', require('./Router/Tracking.router'));
app.use('/api', require('./Router/Banner.router'));
app.use('/api', require('./Router/Promocode.router'));
app.use('/api', require('./Router/SalesAnalysis.router'));
app.use((err, req, res, next) => {
  console.error('🔥 Error:', err.stack || err);
  res.status(500).json({
    success: false,
    error: err.message || 'Internal Server Error',
  });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
