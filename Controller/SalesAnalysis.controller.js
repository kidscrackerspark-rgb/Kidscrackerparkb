const { Pool } = require('pg');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

exports.getSalesAnalysis = async (req, res) => {
  try {
    // === 1. Validate JSONB structure for products ===
    const validateProducts = await pool.query(`
      SELECT 'bookings' AS source, COUNT(*) AS invalid_count
      FROM public.bookings
      WHERE status IN ('booked', 'paid', 'dispatched', 'packed', 'delivered') 
        AND (products IS NULL OR jsonb_typeof(products::jsonb) != 'array')
      UNION ALL
      SELECT 'fwcquotations' AS source, COUNT(*) AS invalid_count
      FROM public.fwcquotations
      WHERE status IN ('booked', 'pending') 
        AND (products IS NULL OR jsonb_typeof(products::jsonb) != 'array')
    `);

    // === 2. Product Sales Summary (from bookings only) ===
    const products = await pool.query(`
      SELECT 
        p.product->>'productname' AS productname,
        COALESCE((p.product->>'quantity')::integer, 0) AS quantity
      FROM public.bookings b
      CROSS JOIN LATERAL jsonb_array_elements(b.products::jsonb) AS p(product)
      WHERE LOWER(b.status) IN ('booked', 'paid', 'dispatched', 'packed', 'delivered')
        AND p.product ? 'productname' AND p.product ? 'quantity'
    `);

    const productSummary = products.rows.reduce((acc, row) => {
      const name = row.productname?.trim();
      const qty = parseInt(row.quantity) || 0;
      if (!name) return acc;
      acc[name] = (acc[name] || 0) + qty;
      return acc;
    }, {});

    const productData = Object.entries(productSummary)
      .map(([productname, quantity]) => ({ productname, quantity }))
      .sort((a, b) => b.quantity - a.quantity);

    // === 3. Regional Demand (Bookings = Orders, Quotations = Leads) ===
    // We separate: Bookings = Confirmed, Quotations = Pipeline
    const cityBookings = await pool.query(`
      SELECT district, COUNT(*) AS count, SUM(COALESCE(total::numeric, 0)) AS total_amount
      FROM public.bookings
      WHERE LOWER(status) IN ('booked', 'paid', 'dispatched', 'packed', 'delivered')
      GROUP BY district
    `);

    const cityQuotations = await pool.query(`
      SELECT district, COUNT(*) AS count, SUM(COALESCE(total::numeric, 0)) AS total_amount
      FROM public.fwcquotations
      WHERE LOWER(status) IN ('pending', 'booked')
      GROUP BY district
    `);

    const combineCityData = (bookingRows, quotationRows, type) => {
      const map = {};
      [...bookingRows, ...quotationRows].forEach(row => {
        const district = row.district || 'Unknown';
        if (!map[district]) {
          map[district] = { 
            district, 
            booking_count: 0, 
            quotation_count: 0, 
            booking_amount: 0, 
            quotation_amount: 0 
          };
        }
        if (type === 'booking') {
          map[district].booking_count += parseInt(row.count);
          map[district].booking_amount += parseFloat(row.total_amount);
        } else {
          map[district].quotation_count += parseInt(row.count);
          map[district].quotation_amount += parseFloat(row.total_amount);
        }
      });
      return Object.values(map);
    };

    const cityData = combineCityData(cityBookings.rows, cityQuotations.rows, 'both');

    // === 4. Historical Trends (Bookings Only) ===
    const historical = await pool.query(`
      SELECT 
        TO_CHAR(DATE_TRUNC('month', created_at), 'YYYY-MM') AS month,
        COUNT(*) AS volume,
        SUM(COALESCE(total::numeric, 0)) AS total_amount,
        SUM(COALESCE(amount_paid::numeric, 0)) AS amount_paid
      FROM public.bookings
      WHERE LOWER(status) IN ('booked', 'paid', 'dispatched', 'packed', 'delivered')
      GROUP BY DATE_TRUNC('month', created_at)
      ORDER BY DATE_TRUNC('month', created_at)
    `);

    const trendDataArray = historical.rows.map(row => {
      const total = parseFloat(row.total_amount) || 0;
      const paid = parseFloat(row.amount_paid) || 0;
      const unpaid = total - paid;

      return {
        month: row.month,
        volume: parseInt(row.volume) || 0,
        total_amount: total,
        amount_paid: Math.min(paid, total), // Cap paid at total
        unpaid_amount: Math.max(0, unpaid)
      };
    });

    // === 5. Profitability (Bookings Only) ===
    const profitability = await pool.query(`
      SELECT 
        SUM(COALESCE(total::numeric, 0)) AS total_amount,
        SUM(COALESCE(amount_paid::numeric, 0)) AS amount_paid
      FROM public.bookings
      WHERE LOWER(status) IN ('booked', 'paid', 'dispatched', 'packed', 'delivered')
    `);

    const profitRow = profitability.rows[0] || { total_amount: 0, amount_paid: 0 };
    let totalAmount = parseFloat(profitRow.total_amount) || 0;
    let amountPaid = parseFloat(profitRow.amount_paid) || 0;

    const profitData = {
      total_amount: totalAmount,
      amount_paid: amountPaid,
      unpaid_amount: Math.max(0, totalAmount - amountPaid)
    };

    // === 6. Quotation Conversion Rates ===
    const quotations = await pool.query(`
      SELECT LOWER(status) AS status, COUNT(*) AS count, SUM(COALESCE(total::numeric, 0)) AS total_amount
      FROM public.fwcquotations
      GROUP BY LOWER(status)
    `);

    const quotationSummary = {
      pending: { count: 0, total_amount: 0 },
      booked: { count: 0, total_amount: 0 },
      canceled: { count: 0, total_amount: 0 }
    };

    quotations.rows.forEach(row => {
      const status = row.status || 'canceled';
      if (quotationSummary[status] !== undefined) {
        quotationSummary[status].count += parseInt(row.count);
        quotationSummary[status].total_amount += parseFloat(row.total_amount) || 0;
      }
    });

    // === 7. Customer Type Analysis (Bookings Only) ===
    const customerTypes = await pool.query(`
      SELECT customer_type, COUNT(*) AS count, SUM(COALESCE(total::numeric, 0)) AS total_amount
      FROM public.bookings
      WHERE LOWER(status) IN ('booked', 'paid', 'dispatched', 'packed', 'delivered') 
        AND customer_type IS NOT NULL
      GROUP BY customer_type
    `);

    const customerTypeData = customerTypes.rows.map(row => ({
      customer_type: row.customer_type || 'Unknown',
      count: parseInt(row.count),
      total_amount: parseFloat(row.total_amount) || 0
    }));

    // === 8. Cancellations ===
    const cancellations = await pool.query(`
      SELECT 'booking' AS type, order_id, COALESCE(total::numeric, 0) AS total, created_at
      FROM public.bookings WHERE LOWER(status) = 'canceled'
      UNION ALL
      SELECT 'quotation' AS type, quotation_id AS order_id, COALESCE(total::numeric, 0) AS total, created_at
      FROM public.fwcquotations WHERE LOWER(status) = 'canceled'
      ORDER BY created_at DESC
    `);

    const cancellationData = cancellations.rows.map(row => ({
      type: row.type,
      order_id: row.order_id,
      total: parseFloat(row.total) || 0,
      created_at: row.created_at
    }));

    // === Final Response ===
    res.status(200).json({
      products: productData,
      cities: cityData,
      trends: trendDataArray,
      profitability: profitData,
      quotations: quotationSummary,
      customer_types: customerTypeData,
      cancellations: cancellationData
    });

  } catch (err) {
    console.error('Failed to fetch sales analysis:', {
      message: err.message,
      stack: err.stack,
      query: err.query || 'N/A'
    });
    res.status(500).json({ message: 'Failed to fetch sales analysis', error: err.message });
  }
};