const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const PDFDocument = require('pdfkit');
const nodemailer = require('nodemailer');

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

const generatePDF = (type, data, customerDetails, products, dbValues) => {
  return new Promise((resolve, reject) => {
    try {
      const doc = new PDFDocument({ margin: 50, size: 'A4' });
      const safeCustomerName = (customerDetails.customer_name || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
      const pdfDir = path.resolve(__dirname, '../pdf_data');
      if (!fs.existsSync(pdfDir)) {
        fs.mkdirSync(pdfDir, { recursive: true });
        fs.chmodSync(pdfDir, 0o770);
      }
      const pdfPath = path.join(pdfDir, `${safeCustomerName}-${data.quotation_id || data.order_id}-${type}.pdf`);
      const stream = fs.createWriteStream(pdfPath, { flags: 'w', mode: 0o660 });
      doc.pipe(stream);

      // Header
      doc.fontSize(20).font('Helvetica-Bold').text(type === 'quotation' ? 'Quotation' : 'Estimate Bill', 50, 50, { align: 'center' });
      doc.fontSize(12).font('Helvetica')
        .text('Kids Crackers Park', 50, 80)
        .text('Sivakasi', 50, 95)
        .text('Mobile: +91 96297 24212', 50, 110)
        .text('Email: kidscrackerspark@gmail.com', 50, 125)
        .text('Website: kidscrackerspark.vercel.app', 50, 140);

      // Customer Details
      const customerType = data.customer_type === 'Customer of Selected Agent' ? 'Customer - Agent' : data.customer_type || 'User';
      let addressLine1 = customerDetails.address || 'N/A';
      let addressLine2 = '';
      if (addressLine1.length > 30) {
        const splitIndex = addressLine1.lastIndexOf(' ', 30);
        addressLine2 = addressLine1.slice(splitIndex + 1);
        addressLine1 = addressLine1.slice(0, splitIndex);
      }
      doc.fontSize(12).font('Helvetica')
        .text(`${type === 'quotation' ? 'Quotation ID' : 'Order ID'}: ${data.quotation_id || data.order_id}`, 300, 80, { align: 'right' })
        .text(`Date: ${new Date().toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric' })}`, 300, 95, { align: 'right' })
        .text(`Customer: ${customerDetails.customer_name || 'N/A'}`, 300, 110, { align: 'right' })
        .text(`Contact: ${customerDetails.mobile_number || 'N/A'}`, 300, 125, { align: 'right' })
        .text(`Address: ${addressLine1}`, 300, 140, { align: 'right' })
        .text(addressLine2, 300, 155, { align: 'right' })
        .text(`District: ${customerDetails.district || 'N/A'}`, 300, 170, { align: 'right' })
        .text(`State: ${customerDetails.state || 'N/A'}`, 300, 185, { align: 'right' })
        .text(`Customer Type: ${customerType}`, 300, 200, { align: 'right' });
      if (data.agent_name) {
        doc.text(`Agent: ${data.agent_name}`, 300, 215, { align: 'right' });
      }

      // Table Setup
      const tableY = 250;
      const tableWidth = 500;
      const colWidths = [30, 150, 50, 70, 70, 50, 100];
      const colX = [50, 80, 210, 250, 320, 400, 450];
      const rowHeight = 25;
      const pageHeight = doc.page.height - doc.page.margins.bottom;

      let y = tableY;

      // Split products into discounted and non-discounted
      const discountedProducts = products.filter(p => parseFloat(p.discount || 0) > 0);
      const netRateProducts = products.filter(p => !p.discount || parseFloat(p.discount) === 0);

      // Primary Table (Discounted Products)
      if (discountedProducts.length > 0) {
        doc.fontSize(12).font('Helvetica-Bold').text('DISCOUNTED PRODUCTS', 50, y - 20);
        doc.moveTo(50, y - 5).lineTo(50 + tableWidth, y - 5).stroke();
        doc.fontSize(10).font('Helvetica-Bold')
          .text('Sl.N', colX[0] + 5, y, { width: colWidths[0] - 10, align: 'center' })
          .text('Product', colX[1] + 5, y, { width: colWidths[1] - 10, align: 'left' })
          .text('Qty', colX[2] + 5, y, { width: colWidths[2] - 10, align: 'center' })
          .text('Rate', colX[3] + 5, y, { width: colWidths[3] - 10, align: 'left' })
          .text('Disc Rate', colX[4] + 5, y, { width: colWidths[4] - 10, align: 'left' })
          .text('Per', colX[5] + 5, y, { width: colWidths[5] - 10, align: 'center' })
          .text('Total', colX[6] + 5, y, { width: colWidths[6] - 10, align: 'left' });
        doc.moveTo(50, y + 15).lineTo(50 + tableWidth, y + 15).stroke();
        colX.forEach((x, i) => {
          doc.moveTo(x, y - 5).lineTo(x, y + 15).stroke();
          if (i === colX.length - 1) {
            doc.moveTo(x + colWidths[i], y - 5).lineTo(x + colWidths[i], y + 15).stroke();
          }
        });

        y += rowHeight;
        discountedProducts.forEach((product, index) => {
          if (y + rowHeight > pageHeight - 50) {
            doc.addPage();
            y = doc.page.margins.top + 20;
            doc.fontSize(12).font('Helvetica-Bold').text('DISCOUNTED PRODUCTS (Continued)', 50, y - 20);
            doc.moveTo(50, y - 5).lineTo(50 + tableWidth, y - 5).stroke();
            doc.fontSize(10).font('Helvetica-Bold')
              .text('Sl.N', colX[0] + 5, y, { width: colWidths[0] - 10, align: 'center' })
              .text('Product', colX[1] + 5, y, { width: colWidths[1] - 10, align: 'left' })
              .text('Qty', colX[2] + 5, y, { width: colWidths[2] - 10, align: 'center' })
              .text('Rate', colX[3] + 5, y, { width: colWidths[3] - 10, align: 'left' })
              .text('Disc Rate', colX[4] + 5, y, { width: colWidths[4] - 10, align: 'left' })
              .text('Per', colX[5] + 5, y, { width: colWidths[5] - 10, align: 'center' })
              .text('Total', colX[6] + 5, y, { width: colWidths[6] - 10, align: 'left' });
            doc.moveTo(50, y + 15).lineTo(50 + tableWidth, y + 15).stroke();
            colX.forEach((x, i) => {
              doc.moveTo(x, y - 5).lineTo(x, y + 15).stroke();
              if (i === colX.length - 1) {
                doc.moveTo(x + colWidths[i], y - 5).lineTo(x + colWidths[i], y + 15).stroke();
              }
            });
            y += rowHeight;
          }

          const price = parseFloat(product.price) || 0;
          const discount = parseFloat(product.discount || 0) || 0;
          const discRate = price - (price * discount / 100);
          const productTotal = discRate * (product.quantity || 1);

          let productName = product.productname || 'N/A';
          if (productName.length > 30) {
            productName = productName.substring(0, 27) + '...';
          }

          doc.font('Helvetica')
            .text(index + 1, colX[0] + 5, y, { width: colWidths[0] - 10, align: 'center' })
            .text(productName, colX[1] + 5, y, { width: colWidths[1] - 10, align: 'left' })
            .text(product.quantity || 1, colX[2] + 5, y, { width: colWidths[2] - 10, align: 'center' })
            .text(`Rs.${price.toFixed(2)}`, colX[3] + 5, y, { width: colWidths[3] - 10, align: 'left' })
            .text(`Rs.${discRate.toFixed(2)}`, colX[4] + 5, y, { width: colWidths[4] - 10, align: 'left' })
            .text(product.per || 'N/A', colX[5] + 5, y, { width: colWidths[5] - 10, align: 'center' })
            .text(`Rs.${productTotal.toFixed(2)}`, colX[6] + 5, y, { width: colWidths[6] - 10, align: 'left' });

          doc.moveTo(50, y + 15).lineTo(50 + tableWidth, y + 15).stroke();
          colX.forEach((x, i) => {
            doc.moveTo(x, y - 5).lineTo(x, y + 15).stroke();
            if (i === colX.length - 1) {
              doc.moveTo(x + colWidths[i], y - 5).lineTo(x + colWidths[i], y + 15).stroke();
            }
          });

          y += rowHeight;
        });
      }

      // Secondary Table (Net Rate Products)
      if (netRateProducts.length > 0) {
        y += 20;
        if (y + rowHeight + 30 > pageHeight - 50) {
          doc.addPage();
          y = doc.page.margins.top + 20;
        }

        doc.fontSize(12).font('Helvetica-Bold').text('NET RATE PRODUCTS', 50, y);
        y += 20;
        doc.moveTo(50, y - 5).lineTo(50 + tableWidth, y - 5).stroke();
        doc.fontSize(10).font('Helvetica-Bold')
          .text('Sl.N', colX[0] + 5, y, { width: colWidths[0] - 10, align: 'center' })
          .text('Product', colX[1] + 5, y, { width: colWidths[1] - 10, align: 'left' })
          .text('Qty', colX[2] + 5, y, { width: colWidths[2] - 10, align: 'center' })
          .text('Rate', colX[3] + 5, y, { width: colWidths[3] - 10, align: 'left' })
          .text('Disc Rate', colX[4] + 5, y, { width: colWidths[4] - 10, align: 'left' })
          .text('Per', colX[5] + 5, y, { width: colWidths[5] - 10, align: 'center' })
          .text('Total', colX[6] + 5, y, { width: colWidths[6] - 10, align: 'left' });
        doc.moveTo(50, y + 15).lineTo(50 + tableWidth, y + 15).stroke();
        colX.forEach((x, i) => {
          doc.moveTo(x, y - 5).lineTo(x, y + 15).stroke();
          if (i === colX.length - 1) {
            doc.moveTo(x + colWidths[i], y - 5).lineTo(x + colWidths[i], y + 15).stroke();
          }
        });

        y += rowHeight;
        netRateProducts.forEach((product, index) => {
          if (y + rowHeight > pageHeight - 50) {
            doc.addPage();
            y = doc.page.margins.top + 20;
            doc.fontSize(12).font('Helvetica-Bold').text('NET RATE PRODUCTS (Continued)', 50, y - 20);
            doc.moveTo(50, y - 5).lineTo(50 + tableWidth, y - 5).stroke();
            doc.fontSize(10).font('Helvetica-Bold')
              .text('Sl.N', colX[0] + 5, y, { width: colWidths[0] - 10, align: 'center' })
              .text('Product', colX[1] + 5, y, { width: colWidths[1] - 10, align: 'left' })
              .text('Qty', colX[2] + 5, y, { width: colWidths[2] - 10, align: 'center' })
              .text('Rate', colX[3] + 5, y, { width: colWidths[3] - 10, align: 'left' })
              .text('Disc Rate', colX[4] + 5, y, { width: colWidths[4] - 10, align: 'left' })
              .text('Per', colX[5] + 5, y, { width: colWidths[5] - 10, align: 'center' })
              .text('Total', colX[6] + 5, y, { width: colWidths[6] - 10, align: 'left' });
            doc.moveTo(50, y + 15).lineTo(50 + tableWidth, y + 15).stroke();
            colX.forEach((x, i) => {
              doc.moveTo(x, y - 5).lineTo(x, y + 15).stroke();
              if (i === colX.length - 1) {
                doc.moveTo(x + colWidths[i], y - 5).lineTo(x + colWidths[i], y + 15).stroke();
              }
            });
            y += rowHeight;
          }

          const price = parseFloat(product.price) || 0;
          const discRate = price; // No discount for net rate products
          const productTotal = discRate * (product.quantity || 1);

          let productName = product.productname || 'N/A';
          if (productName.length > 30) {
            productName = productName.substring(0, 27) + '...';
          }

          doc.font('Helvetica')
            .text(index + 1, colX[0] + 5, y, { width: colWidths[0] - 10, align: 'center' })
            .text(productName, colX[1] + 5, y, { width: colWidths[1] - 10, align: 'left' })
            .text(product.quantity || 1, colX[2] + 5, y, { width: colWidths[2] - 10, align: 'center' })
            .text(`Rs.${price.toFixed(2)}`, colX[3] + 5, y, { width: colWidths[3] - 10, align: 'left' })
            .text(`Rs.${discRate.toFixed(2)}`, colX[4] + 5, y, { width: colWidths[4] - 10, align: 'left' })
            .text(product.per || 'N/A', colX[5] + 5, y, { width: colWidths[5] - 10, align: 'center' })
            .text(`Rs.${productTotal.toFixed(2)}`, colX[6] + 5, y, { width: colWidths[6] - 10, align: 'left' });

          doc.moveTo(50, y + 15).lineTo(50 + tableWidth, y + 15).stroke();
          colX.forEach((x, i) => {
            doc.moveTo(x, y - 5).lineTo(x, y + 15).stroke();
            if (i === colX.length - 1) {
              doc.moveTo(x + colWidths[i], y - 5).lineTo(x + colWidths[i], y + 15).stroke();
            }
          });

          y += rowHeight;
        });
      }

      // Handle case with no products
      if (products.length === 0) {
        y += 20;
        doc.fontSize(12).font('Helvetica').text('No products available', 50, y, { align: 'center' });
        y += 20;
      }

      // Totals Section
      if (y + 90 > pageHeight - 50) {
        doc.addPage();
        y = doc.page.margins.top + 20;
      }

      const netRate = parseFloat(dbValues.net_rate) || 0;
      const youSave = parseFloat(dbValues.you_save) || 0;
      const additionalDiscount = parseFloat(dbValues.additional_discount || 0); // Ensure correct parsing
      const subtotal = netRate - youSave; // Subtotal after product discounts
      const additionalDiscountAmount = subtotal * (additionalDiscount / 100); // Calculate additional discount
      const grandTotal = subtotal - additionalDiscountAmount; // Final total

      doc.fontSize(10).font('Helvetica-Bold')
        .text(`Net Rate: Rs.${netRate.toFixed(2)}`, 350, y, { width: 150, align: 'right' });
      y += 20;
      doc.text(`You Save: Rs.${youSave.toFixed(2)}`, 350, y, { width: 150, align: 'right' });
      y += 20;
      
      // Conditionally display Additional Discount if > 0
      if (additionalDiscount > 0) {
        doc.text(`Additional Discount (${additionalDiscount.toFixed(2)}%): Rs.${additionalDiscountAmount.toFixed(2)}`, 350, y, { width: 150, align: 'right' });
        y += 20;
      }
      
      doc.text(`Grand Total: Rs.${grandTotal.toFixed(2)}`, 350, y, { width: 150, align: 'right' });
      y += 30;

      if (y + 50 > pageHeight - 50) {
        doc.addPage();
        y = doc.page.margins.top + 20;
      }
      doc.fontSize(10).font('Helvetica')
        .text('Thank you for your business! Dear customers, the logistics payments are to be paid by yourself', 50, y, { align: 'center' })
        .text('For any queries, contact us at +91 96297 24212', 50, y + 15, { align: 'center' });

      doc.end();
      stream.on('finish', () => {
        const stats = fs.statSync(pdfPath);
        if (stats.size === 0) {
          fs.unlinkSync(pdfPath);
          reject(new Error('Generated PDF is empty'));
        } else {
          console.log(`PDF generated successfully: ${pdfPath}`);
          resolve({ pdfPath, calculatedTotal: grandTotal });
        }
      });
      stream.on('error', (err) => {
        console.error(`PDF generation failed for ${pdfPath}:`, err.message);
        reject(err);
      });
    } catch (err) {
      console.error(`Error in generatePDF for ${type} ${data.quotation_id || data.order_id}:`, err.message);
      reject(err);
    }
  });
};

async function sendBookingEmail(toEmail, bookingData, customerDetails, pdfPath, products, type, status = 'booked', transportDetails = null) {
  try {
    if (!fs.existsSync(pdfPath)) {
      console.error(`PDF file does not exist for email: ${pdfPath}`);
      throw new Error('PDF file does not exist for email');
    }
    const transporter = nodemailer.createTransport({
      service: 'gmail',
      auth: {
        user: 'kidscrackerspark@gmail.com',
        pass: 'mwmv tccc thsb ieyi'
      },
      pool: true,
      maxConnections: 5,
      connectionTimeout: 2 * 60 * 1000,
      greetingTimeout: 2 * 60 * 1000,
      socketTimeout: 2 * 60 * 1000,
      logger: true,
      debug: true
    });

    const productList = products.map(p =>
      `- ${p.productname || 'N/A'}: ${p.quantity || 1} x Rs.${parseFloat(p.price || 0).toFixed(2)}`
    ).join('\n');

    let subject, text, html;
    const idField = type === 'quotation' ? 'Quotation ID' : 'Order ID';
    const idValue = bookingData.quotation_id || bookingData.order_id;

    if (toEmail === 'kidscrackerspark@gmail.com' && type === 'invoice' && status === 'booked') {
      subject = `New Booking Notification: Order ${idValue}`;
      text = `
A new booking has been made with Phoenix Crackers.

Booking Details:
${idField}: ${idValue}
Customer Name: ${customerDetails.customer_name || 'N/A'}
Mobile: ${customerDetails.mobile_number || 'N/A'}
Email: ${customerDetails.email || 'N/A'}
Address: ${customerDetails.address || 'N/A'}
District: ${customerDetails.district || 'N/A'}
State: ${customerDetails.state || 'N/A'}
Customer Type: ${bookingData.customer_type || 'User'}
Net Rate: Rs.${parseFloat(bookingData.net_rate || 0).toFixed(2)}
You Save: Rs.${parseFloat(bookingData.you_save || 0).toFixed(2)}
Total: Rs.${parseFloat(bookingData.total || 0).toFixed(2)}

Attached is the estimate bill for reference.

Best regards,
Phoenix Crackers Team
      `;
    } else if (toEmail === 'kidscrackerspark@gmail.com' && type === 'invoice' && status === 'paid') {
      subject = `New Payment Notification: Order ${idValue}`;
      text = `
A payment has been received for Order ${idValue}.

Customer Name: ${customerDetails.customer_name || 'N/A'}
Order ID: ${idValue}
Total: Rs.${parseFloat(bookingData.total || 0).toFixed(2)}

Attached is the estimate bill for reference.

Best regards,
Phoenix Crackers Team
      `;
    } else if (type === 'invoice' && status === 'booked') {
      subject = `Thank You for Your Booking! Order ${idValue}`;
      text = `
Dear ${customerDetails.customer_name || 'Customer'},

Thank you for your booking with Phoenix Crackers!

Booking Details:
${idField}: ${idValue}
Customer Name: ${customerDetails.customer_name || 'N/A'}
Mobile: ${customerDetails.mobile_number || 'N/A'}
Email: ${customerDetails.email || 'N/A'}
Address: ${customerDetails.address || 'N/A'}
District: ${customerDetails.district || 'N/A'}
State: ${customerDetails.state || 'N/A'}
Customer Type: ${bookingData.customer_type || 'User'}
Total: Rs.${parseFloat(bookingData.total || 0).toFixed(2)}

Please make the payment to the following UPI ID: 2417805A@sib
After making the payment, kindly send the transaction screenshot along with your username to our WhatsApp number: +91 63836 59214.

Attached is your estimate bill for reference.

For any queries, contact us at +91 63836 59214.

Best regards,
Phoenix Crackers Team
      `;
    } else if (type === 'invoice' && status === 'paid') {
      subject = `Payment Received for Order ${idValue}`;
      text = `
Dear ${customerDetails.customer_name || 'Customer'},

Thank you for your payment for Order ${idValue}!

We have received your payment, and we will start packing your order soon.

Booking Details:
${idField}: ${idValue}
Customer Name: ${customerDetails.customer_name || 'N/A'}
Mobile: ${customerDetails.mobile_number || 'N/A'}
Email: ${customerDetails.email || 'N/A'}
Address: ${customerDetails.address || 'N/A'}
District: ${customerDetails.district || 'N/A'}
State: ${customerDetails.state || 'N/A'}
Customer Type: ${bookingData.customer_type || 'User'}
Net Rate: Rs.${parseFloat(bookingData.net_rate || 0).toFixed(2)}
You Save: Rs.${parseFloat(bookingData.you_save || 0).toFixed(2)}
Total: Rs.${parseFloat(bookingData.total || 0).toFixed(2)}

Products:
${productList}

Attached is your estimate bill for reference.

For any queries, contact us at +91 63836 59214.

Best regards,
Phoenix Crackers Team

Transport Details:
${transportDetails ? Object.entries(transportDetails).map(([key, value]) => `${key}: ${value}`).join('\n') : 'N/A'}

Booking Details:
${idField}: ${idValue}
Customer Name: ${customerDetails.customer_name || 'N/A'}
Mobile: ${customerDetails.mobile_number || 'N/A'}
Email: ${customerDetails.email || 'N/A'}
Address: ${customerDetails.address || 'N/A'}
District: ${customerDetails.district || 'N/A'}
State: ${customerDetails.state || 'N/A'}
Customer Type: ${bookingData.customer_type || 'User'}
Net Rate: Rs.${parseFloat(bookingData.net_rate || 0).toFixed(2)}
You Save: Rs.${parseFloat(bookingData.you_save || 0).toFixed(2)}
Total: Rs.${parseFloat(bookingData.total || 0).toFixed(2)}

Products:
${productList}

Attached is your estimate bill for reference.

For any queries, contact us at +91 63836 59214.

Best regards,
Phoenix Crackers Team
      `;
    } else {
      subject = `New ${type === 'quotation' ? 'Quotation' : 'Booking'}: ${idValue}`;
      text = `
A new ${type} has been made.

Customer Name: ${customerDetails.customer_name || 'N/A'}
Mobile: ${customerDetails.mobile_number || 'N/A'}
Email: ${customerDetails.email || 'N/A'}
Address: ${customerDetails.address || 'N/A'}
District: ${customerDetails.district || 'N/A'}
State: ${customerDetails.state || 'N/A'}
${idField}: ${idValue}
Customer Type: ${bookingData.customer_type || 'User'}
Net Rate: Rs.${parseFloat(bookingData.net_rate || 0).toFixed(2)}
You Save: Rs.${parseFloat(bookingData.you_save || 0).toFixed(2)}
Total: Rs.${parseFloat(bookingData.total || 0).toFixed(2)}

Products:
${productList}
      `;
    }

    const mailOptions = {
      from: '"Phoenix Crackers" <kidscrackerspark@gmail.com>',
      to: toEmail,
      subject,
      text,
      html,
      attachments: [
        {
          filename: path.basename(pdfPath),
          path: pdfPath,
          contentType: 'application/pdf',
        }
      ]
    };

    await transporter.sendMail(mailOptions);
  } catch (err) {
    console.error('Failed to send email:', err.message);
    throw err;
  }
}

exports.getCustomers = async (req, res) => {
  try {
    const query = `
      SELECT c.id, c.customer_name AS name, c.address, c.mobile_number, c.email, c.customer_type, c.district, c.state, c.agent_id,
             a.customer_name AS agent_name
      FROM public.customers c
      LEFT JOIN public.customers a ON c.agent_id::bigint = a.id AND c.customer_type = 'Customer of Selected Agent'
    `;
    const result = await pool.query(query);
    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Failed to fetch customers:', err.stack);
    res.status(500).json({ error: 'Failed to fetch customers', details: err.message });
  }
};

exports.getProductTypes = async (req, res) => {
  try {
    const result = await pool.query('SELECT DISTINCT product_type FROM public.products');
    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Failed to fetch product types:', err.message);
    res.status(500).json({ message: 'Failed to fetch product types', error: err.message });
  }
};

exports.getProductsByType = async (req, res) => {
  try {
    const productTypesResult = await pool.query('SELECT DISTINCT product_type FROM public.products');
    const productTypes = productTypesResult.rows.map(row => row.product_type);
    let allProducts = [];
    for (const productType of productTypes) {
      const tableName = productType.toLowerCase().replace(/\s+/g, '_');
      const query = `
        SELECT id, serial_number, productname, price, per, discount, image, status, $1 AS product_type
        FROM public.${tableName}
        WHERE status = 'on'
      `;
      const result = await pool.query(query, [productType]);
      allProducts = allProducts.concat(result.rows);
    }
    const products = allProducts.map(row => ({
      id: row.id,
      product_type: row.product_type,
      serial_number: row.serial_number,
      productname: row.productname,
      price: parseFloat(row.price || 0),
      per: row.per,
      discount: parseFloat(row.discount || 0),
      image: row.image,
      status: row.status
    }));
    res.status(200).json(products);
  } catch (err) {
    console.error('Failed to fetch products:', err.message);
    res.status(500).json({ message: 'Failed to fetch products', error: err.message });
  }
};

exports.getAproductsByType = async (req, res) => {
  try {
    const productTypesResult = await pool.query('SELECT DISTINCT product_type FROM public.products');
    const productTypes = productTypesResult.rows.map(row => row.product_type);
    let allProducts = [];
    for (const productType of productTypes) {
      const tableName = productType.toLowerCase().replace(/\s+/g, '_');
      const query = `
        SELECT id, serial_number, productname, price, per, discount, image, status, $1 AS product_type
        FROM public.${tableName}
      `;
      const result = await pool.query(query, [productType]);
      allProducts = allProducts.concat(result.rows);
    }
    const products = allProducts.map(row => ({
      id: row.id,
      product_type: row.product_type,
      serial_number: row.serial_number,
      productname: row.productname,
      price: parseFloat(row.price || 0),
      per: row.per,
      discount: parseFloat(row.discount || 0),
      image: row.image,
      status: row.status
    }));
    res.status(200).json(products);
  } catch (err) {
    console.error('Failed to fetch products:', err.message);
    res.status(500).json({ message: 'Failed to fetch products', error: err.message });
  }
};

exports.getAllQuotations = async (req, res) => {
  try {
    const query = `
      SELECT id, customer_id, quotation_id, products, net_rate, you_save, total, promo_discount, 
             customer_name, address, mobile_number, email, district, state, customer_type, 
             status, created_at, updated_at, pdf
      FROM public.fwcquotations
      ORDER BY created_at DESC
    `;
    const result = await pool.query(query);
    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Failed to fetch quotations:', err.message);
    res.status(500).json({ message: 'Failed to fetch quotations', error: err.message });
  }
};

exports.createQuotation = async (req, res) => {
  try {
    const {
      customer_id, quotation_id, products, net_rate, you_save, total, promo_discount, additional_discount,
      customer_type, customer_name, address, mobile_number, email, district, state
    } = req.body;

    if (!quotation_id || !/^[a-zA-Z0-9-_]+$/.test(quotation_id)) 
      return res.status(400).json({ message: 'Invalid or missing Quotation ID' });
    
    if (!Array.isArray(products) || products.length === 0) 
      return res.status(400).json({ message: 'Products array is required and must not be empty' });
    
    if (!total || isNaN(parseFloat(total)) || parseFloat(total) <= 0) 
      return res.status(400).json({ message: 'Total must be a positive number' });

    const parsedNetRate = parseFloat(net_rate) || 0;
    const parsedYouSave = parseFloat(you_save) || 0;
    const parsedPromoDiscount = parseFloat(promo_discount) || 0;
    const parsedAdditionalDiscount = parseFloat(additional_discount) || 0;
    const parsedTotal = parseFloat(total);

    if ([parsedNetRate, parsedYouSave, parsedPromoDiscount, parsedAdditionalDiscount, parsedTotal].some(v => isNaN(v)))
      return res.status(400).json({ message: 'net_rate, you_save, promo_discount, additional_discount, and total must be valid numbers' });

    let finalCustomerType = customer_type || 'User';
    let customerDetails = { customer_name, address, mobile_number, email, district, state };
    let agent_name = null;

    if (customer_id) {
      const customerCheck = await pool.query(
        'SELECT id, customer_name, address, mobile_number, email, district, state, customer_type, agent_id FROM public.customers WHERE id = $1',
        [customer_id]
      );
      if (customerCheck.rows.length === 0) 
        return res.status(404).json({ message: 'Customer not found' });

      const customerRow = customerCheck.rows[0];
      finalCustomerType = customer_type || customerRow.customer_type || 'User';
      customerDetails = {
        customer_name: customerRow.customer_name,
        address: customerRow.address,
        mobile_number: customerRow.mobile_number,
        email: customerRow.email,
        district: customerRow.district,
        state: customerRow.state
      };

      if (finalCustomerType === 'Customer of Selected Agent' && customerRow.agent_id) {
        const agentCheck = await pool.query('SELECT customer_name FROM public.customers WHERE id = $1', [customerRow.agent_id]);
        if (agentCheck.rows.length > 0) agent_name = agentCheck.rows[0].customer_name;
      }
    } else {
      if (finalCustomerType !== 'User') 
        return res.status(400).json({ message: 'Customer type must be "User" for quotations without customer ID' });
      if (!customer_name || !address || !district || !state || !mobile_number)
        return res.status(400).json({ message: 'All customer details must be provided' });
    }

    for (const product of products) {
      const { id, product_type, quantity, price, discount, additional_discount } = product;
      if (!id || !product_type || quantity < 1 || isNaN(parseFloat(price)) || isNaN(parseFloat(discount)) || isNaN(parseFloat(additional_discount)))
        return res.status(400).json({ message: 'Invalid product entry' });

      const tableName = product_type.toLowerCase().replace(/\s+/g, '_');
      const productCheck = await pool.query(`SELECT id FROM public.${tableName} WHERE id = $1`, [id]);
      if (productCheck.rows.length === 0)
        return res.status(404).json({ message: `Product ${id} of type ${product_type} not found or unavailable` });
    }

    const { pdfPath } = await generatePDF(
      'quotation',
      { quotation_id, customer_type: finalCustomerType, total: parsedTotal, agent_name },
      customerDetails,
      products,
      { 
        net_rate: parsedNetRate, 
        you_save: parsedYouSave, 
        total: parsedTotal, 
        promo_discount: parsedPromoDiscount,
        additional_discount: parsedAdditionalDiscount // Ensure passed correctly
      }
    );

    await pool.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
    await pool.query('BEGIN');

    const quotationResult = await pool.query(`
      INSERT INTO public.fwcquotations 
      (customer_id, quotation_id, products, net_rate, you_save, total, promo_discount, additional_discount, address, mobile_number, customer_name, email, district, state, customer_type, status, created_at, pdf)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NOW(), $17)
      RETURNING id, created_at, customer_type, pdf, quotation_id
    `, [
      customer_id || null,
      quotation_id,
      JSON.stringify(products),
      parsedNetRate,
      parsedYouSave,
      parsedTotal,
      parsedPromoDiscount,
      parsedAdditionalDiscount,
      customerDetails.address || null,
      customerDetails.mobile_number || null,
      customerDetails.customer_name || null,
      customerDetails.email || null,
      customerDetails.district || null,
      customerDetails.state || null,
      finalCustomerType,
      'pending',
      pdfPath
    ]);

    await pool.query('COMMIT');

    try {
      await sendQuotationEmail(
        customerDetails.email || 'kidscrackerspark@gmail.com',
        {
          quotation_id,
          customer_type: finalCustomerType,
          net_rate: parsedNetRate,
          you_save: parsedYouSave,
          total: parsedTotal
        },
        customerDetails,
        pdfPath,
        products,
        'quotation',
        'pending'
      );
      await sendQuotationEmail(
        'kidscrackerspark@gmail.com',
        {
          quotation_id,
          customer_type: finalCustomerType,
          net_rate: parsedNetRate,
          you_save: parsedYouSave,
          total: parsedTotal
        },
        customerDetails,
        pdfPath,
        products,
        'quotation',
        'pending'
      );
    } catch (emailErr) {
      console.error(`Failed to send email for quotation ${quotation_id}:`, emailErr.message);
    }

    res.status(201).json({
      message: 'Quotation created successfully',
      id: quotationResult.rows[0].id,
      created_at: quotationResult.rows[0].created_at,
      customer_type: quotationResult.rows[0].customer_type,
      pdf_path: quotationResult.rows[0].pdf,
      quotation_id: quotationResult.rows[0].quotation_id,
      pdf_url: `/quotation/${quotation_id}`
    });
  } catch (err) {
    await pool.query('ROLLBACK');
    console.error(`Failed to create quotation for quotation_id ${req.body.quotation_id}:`, err.message);
    res.status(500).json({ message: 'Failed to create quotation', error: err.message });
  }
};

exports.updateQuotation = async (req, res) => {
  try {
    const { quotation_id } = req.params;
    const { products, net_rate, you_save, total, promo_discount, additional_discount, status } = req.body;

    if (!quotation_id || !/^[a-zA-Z0-9-_]+$/.test(quotation_id)) return res.status(400).json({ message: 'Invalid or missing Quotation ID' });
    if (products && (!Array.isArray(products) || products.length === 0)) return res.status(400).json({ message: 'Products array is required and must not be empty' });
    if (total && (isNaN(parseFloat(total)) || parseFloat(total) <= 0)) return res.status(400).json({ message: 'Total must be a positive number' });
    if (status && !['pending', 'booked', 'canceled'].includes(status)) return res.status(400).json({ message: 'Invalid status' });

    const parsedNetRate = net_rate !== undefined ? parseFloat(net_rate) : undefined;
    const parsedYouSave = you_save !== undefined ? parseFloat(you_save) : undefined;
    const parsedPromoDiscount = promo_discount !== undefined ? parseFloat(promo_discount) : undefined;
    const parsedAdditionalDiscount = additional_discount !== undefined ? parseFloat(additional_discount) : undefined;
    const parsedTotal = total !== undefined ? parseFloat(total) : undefined;

    if ([parsedNetRate, parsedYouSave, parsedPromoDiscount, parsedAdditionalDiscount, parsedTotal].some(v => v !== undefined && isNaN(v)))
      return res.status(400).json({ message: 'net_rate, you_save, total, and promo_discount and additional_discount must be valid numbers' });

    const quotationCheck = await pool.query(
      'SELECT * FROM public.fwcquotations WHERE quotation_id = $1',
      [quotation_id]
    );
    if (quotationCheck.rows.length === 0) return res.status(404).json({ message: 'Quotation not found' });

    const quotation = quotationCheck.rows[0];
    let customerDetails = {
      customer_name: quotation.customer_name,
      address: quotation.address,
      mobile_number: quotation.mobile_number,
      email: quotation.email,
      district: quotation.district,
      state: quotation.state
    };
    let agent_name = null;

    if (quotation.customer_id) {
      const customerCheck = await pool.query(
        'SELECT customer_name, address, mobile_number, email, district, state, customer_type, agent_id FROM public.customers WHERE id = $1',
        [quotation.customer_id]
      );
      if (customerCheck.rows.length > 0) {
        customerDetails = customerCheck.rows[0];
        if (customerDetails.customer_type === 'Customer of Selected Agent' && customerDetails.agent_id) {
          const agentCheck = await pool.query('SELECT customer_name FROM public.customers WHERE id = $1', [customerDetails.agent_id]);
          if (agentCheck.rows.length > 0) agent_name = agentCheck.rows[0].customer_name;
        }
      }
    }

    if (products) {
      for (const product of products) {
        const { id, product_type, quantity, price, discount, additional_discount } = product;
        if (!id || !product_type || quantity < 1 || isNaN(parseFloat(price)) || isNaN(parseFloat(discount)) || isNaN(parseFloat(additional_discount)))
          return res.status(400).json({ message: 'Invalid product entry' });

        const tableName = product_type.toLowerCase().replace(/\s+/g, '_');
        const productCheck = await pool.query(`SELECT id FROM public.${tableName} WHERE id = $1`, [id]);
        if (productCheck.rows.length === 0)
          return res.status(404).json({ message: `Product ${id} of type ${product_type} not found or unavailable` });
      }
    }

    let pdfPath = quotation.pdf;
    if (products && parsedTotal !== undefined) {
      const pdfResult = await generatePDF(
        'quotation',
        { quotation_id, customer_type: quotation.customer_type, total: parsedTotal, agent_name },
        customerDetails,
        products,
        {
          net_rate: parsedNetRate !== undefined ? parsedNetRate : parseFloat(quotation.net_rate || 0),
          you_save: parsedYouSave !== undefined ? parsedYouSave : parseFloat(quotation.you_save || 0),
          total: parsedTotal !== undefined ? parsedTotal : parseFloat(quotation.total || 0),
          promo_discount: parsedPromoDiscount !== undefined ? parsedPromoDiscount : parseFloat(quotation.promo_discount || 0),
          additional_discount: parsedAdditionalDiscount !== undefined ? parsedAdditionalDiscount : parseFloat(quotation.additional_discount || 0)
        }
      );
      pdfPath = pdfResult.pdfPath;
    }

    const updateFields = [];
    const updateValues = [];
    let paramIndex = 1;

    if (products) {
      updateFields.push(`products = $${paramIndex++}`);
      updateValues.push(JSON.stringify(products));
    }
    if (parsedNetRate !== undefined) {
      updateFields.push(`net_rate = $${paramIndex++}`);
      updateValues.push(parsedNetRate);
    }
    if (parsedYouSave !== undefined) {
      updateFields.push(`you_save = $${paramIndex++}`);
      updateValues.push(parsedYouSave);
    }
    if (parsedTotal !== undefined) {
      updateFields.push(`total = $${paramIndex++}`);
      updateValues.push(parsedTotal);
    }
    if (parsedPromoDiscount !== undefined) {
      updateFields.push(`promo_discount = $${paramIndex++}`);
      updateValues.push(parsedPromoDiscount);
    }
    if (parsedAdditionalDiscount !== undefined) {
      updateFields.push(`additional_discount = $${paramIndex++}`);
      updateValues.push(parsedAdditionalDiscount);
    }
    if (pdfPath) {
      updateFields.push(`pdf = $${paramIndex++}`);
      updateValues.push(pdfPath);
    }
    if (status) {
      updateFields.push(`status = $${paramIndex++}`);
      updateValues.push(status);
    }
    updateFields.push(`updated_at = NOW()`);

    if (updateFields.length === 1) {
      return res.status(400).json({ message: 'No fields to update' });
    }

    const query = `
      UPDATE public.fwcquotations 
      SET ${updateFields.join(', ')}
      WHERE quotation_id = $${paramIndex}
      RETURNING id, quotation_id, status
    `;
    updateValues.push(quotation_id);

    const result = await pool.query(query, updateValues);

    res.status(200).json({
      message: 'Quotation updated successfully',
      id: result.rows[0].id,
      quotation_id: result.rows[0].quotation_id,
      status: result.rows[0].status
    });
  } catch (err) {
    console.error(`Failed to update quotation ${req.params.quotation_id}:`, err.message);
    res.status(500).json({ message: 'Failed to update quotation', error: err.message });
  }
};

exports.deleteQuotation = async (req, res) => {
  try {
    const { quotation_id } = req.params;
    if (!quotation_id || !/^[a-zA-Z0-9-_]+$/.test(quotation_id)) return res.status(400).json({ message: 'Invalid or missing Quotation ID' });

    const quotationCheck = await pool.query(
      'SELECT * FROM public.fwcquotations WHERE quotation_id = $1 AND status = $2',
      [quotation_id, 'pending']
    );
    if (quotationCheck.rows.length === 0) return res.status(404).json({ message: 'Quotation not found or not in pending status' });

    await pool.query(
      'UPDATE public.fwcquotations SET status = $1, updated_at = NOW() WHERE quotation_id = $2',
      ['canceled', quotation_id]
    );

    res.status(200).json({ message: 'Quotation canceled successfully', quotation_id });
  } catch (err) {
    console.error(`Failed to cancel quotation ${req.params.quotation_id}:`, err.message);
    res.status(500).json({ message: 'Failed to cancel quotation', error: err.message });
  }
};

exports.getQuotation = async (req, res) => {
  try {
    let { quotation_id } = req.params;
    if (quotation_id.endsWith('.pdf')) quotation_id = quotation_id.replace(/\.pdf$/, '');
    if (!/^[a-zA-Z0-9-_]+$/.test(quotation_id)) return res.status(400).json({ message: 'Invalid quotation_id format' });

    let quotationQuery = await pool.query(
      'SELECT products, net_rate, you_save, total, promo_discount, additional_discount, customer_name, address, mobile_number, email, district, state, customer_type, pdf, customer_id, status FROM public.fwcquotations WHERE quotation_id = $1',
      [quotation_id]
    );

    if (quotationQuery.rows.length === 0) {
      const parts = quotation_id.split('-');
      if (parts.length > 1) {
        const possibleQuotationId = parts.slice(1).join('-');
        quotationQuery = await pool.query(
          'SELECT products, net_rate, you_save, total, promo_discount, additional_discount, customer_name, address, mobile_number, email, district, state, customer_type, pdf, customer_id, status FROM public.fwcquotations WHERE quotation_id = $1',
          [possibleQuotationId]
        );
      }
    }

    if (quotationQuery.rows.length === 0) {
      console.error(`Quotation not found for ID: ${quotation_id}`);
      return res.status(404).json({ message: 'Quotation not found' });
    }

    const { products, net_rate, you_save, total, promo_discount, customer_name, address, mobile_number, email, district, state, customer_type, pdf, customer_id, status } = quotationQuery.rows[0];

    let agent_name = null;
    if (customer_type === 'Customer of Selected Agent' && customer_id) {
      const customerCheck = await pool.query('SELECT agent_id FROM public.customers WHERE id = $1', [customer_id]);
      if (customerCheck.rows.length > 0 && customerCheck.rows[0].agent_id) {
        const agentCheck = await pool.query('SELECT customer_name FROM public.customers WHERE id = $1', [customerCheck.rows[0].agent_id]);
        if (agentCheck.rows.length > 0) agent_name = agentCheck.rows[0].customer_name;
      }
    }

    let pdfPath = pdf;
    if (!fs.existsSync(pdf)) {
      console.log(`PDF not found at ${pdf}, regenerating for quotation ${quotation_id}`);
      const parsedProducts = typeof products === 'string' ? JSON.parse(products) : products;
      const pdfResult = await generatePDF(
        'quotation',
        { quotation_id, customer_type, total: parseFloat(total || 0), agent_name },
        { customer_name, address, mobile_number, email, district, state },
        parsedProducts,
        { 
          net_rate: parseFloat(net_rate || 0), 
          you_save: parseFloat(you_save || 0), 
          total: parseFloat(total || 0), 
          promo_discount: parseFloat(promo_discount || 0),
          additional_discount: parseFloat(additional_discount || 0)
        }
      );
      pdfPath = pdfResult.pdfPath;

      await pool.query(
        'UPDATE public.fwcquotations SET pdf = $1 WHERE quotation_id = $2',
        [pdfPath, quotation_id]
      );
    }

    if (!fs.existsSync(pdfPath)) {
      console.error(`PDF file not found after generation: ${pdfPath}`);
      return res.status(500).json({ message: 'PDF file not found after generation', error: 'File system error' });
    }

    const safeCustomerName = (customer_name || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename=${safeCustomerName}-${quotation_id}-quotation.pdf`);
    fs.createReadStream(pdfPath).pipe(res);
  } catch (err) {
    console.error(`Failed to fetch quotation ${req.params.quotation_id}:`, err.message);
    res.status(500).json({ message: 'Failed to fetch quotation', error: err.message });
  }
};

exports.createBooking = async (req, res) => {
  try {
    const {
      customer_id, order_id, quotation_id, products, net_rate, you_save, total, promo_discount, additional_discount,
      customer_type, customer_name, address, mobile_number, email, district, state
    } = req.body;

    if (!order_id || !/^[a-zA-Z0-9-_]+$/.test(order_id)) 
      return res.status(400).json({ message: 'Invalid or missing Order ID' });
    
    if (!Array.isArray(products) || products.length === 0) 
      return res.status(400).json({ message: 'Products array is required and must not be empty' });
    
    if (!total || isNaN(parseFloat(total)) || parseFloat(total) <= 0) 
      return res.status(400).json({ message: 'Total must be a positive number' });

    const parsedNetRate = parseFloat(net_rate) || 0;
    const parsedYouSave = parseFloat(you_save) || 0;
    const parsedPromoDiscount = parseFloat(promo_discount) || 0;
    const parsedAdditionalDiscount = parseFloat(additional_discount) || 0;
    const parsedTotal = parseFloat(total);

    if ([parsedNetRate, parsedYouSave, parsedPromoDiscount, parsedAdditionalDiscount, parsedTotal].some(v => isNaN(v)))
      return res.status(400).json({ message: 'net_rate, you_save, promo_discount, additional_discount, and total must be valid numbers' });

    let finalCustomerType = customer_type || 'User';
    let customerDetails = { customer_name, address, mobile_number, email, district, state };

    if (customer_id) {
      const customerCheck = await pool.query(
        'SELECT id, customer_name, address, mobile_number, email, district, state, customer_type FROM public.customers WHERE id = $1',
        [customer_id]
      );
      if (customerCheck.rows.length === 0) 
        return res.status(404).json({ message: 'Customer not found' });

      const customerRow = customerCheck.rows[0];
      finalCustomerType = customer_type || customerRow.customer_type || 'User';
      customerDetails = {
        customer_name: customerRow.customer_name,
        address: customerRow.address,
        mobile_number: customerRow.mobile_number,
        email: customerRow.email,
        district: customerRow.district,
        state: customerRow.state
      };
    }

    for (const product of products) {
      const { id, product_type, quantity, price, discount, additional_discount } = product;
      if (!id || !product_type || quantity < 1 || isNaN(parseFloat(price)) || isNaN(parseFloat(discount)) || isNaN(parseFloat(additional_discount)))
        return res.status(400).json({ message: 'Invalid product entry' });

      const tableName = product_type.toLowerCase().replace(/\s+/g, '_');
      const productCheck = await pool.query(`SELECT id FROM public.${tableName} WHERE id = $1`, [id]);
      if (productCheck.rows.length === 0)
        return res.status(404).json({ message: `Product ${id} of type ${product_type} not found or unavailable` });
    }

    const { pdfPath } = await generatePDF(
      'invoice',
      { order_id, customer_type: finalCustomerType, total: parsedTotal },
      customerDetails,
      products,
      { 
        net_rate: parsedNetRate, 
        you_save: parsedYouSave, 
        total: parsedTotal, 
        promo_discount: parsedPromoDiscount,
        additional_discount: parsedAdditionalDiscount
      }
    );

    await pool.query('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');
    await pool.query('BEGIN');

    const bookingResult = await pool.query(`
      INSERT INTO public.bookings 
      (customer_id, order_id, quotation_id, products, net_rate, you_save, total, promo_discount, additional_discount, address, mobile_number, customer_name, email, district, state, customer_type, status, created_at, pdf)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW(), $18)
      RETURNING id, created_at, customer_type, pdf, order_id
    `, [
      customer_id || null,
      order_id,
      quotation_id || null,
      JSON.stringify(products),
      parsedNetRate,
      parsedYouSave,
      parsedTotal,
      parsedPromoDiscount,
      parsedAdditionalDiscount,
      customerDetails.address || null,
      customerDetails.mobile_number || null,
      customerDetails.customer_name || null,
      customerDetails.email || null,
      customerDetails.district || null,
      customerDetails.state || null,
      finalCustomerType,
      'booked',
      pdfPath
    ]);

    if (quotation_id) {
      const quotationCheck = await pool.query(
        'SELECT id FROM public.fwcquotations WHERE quotation_id = $1 AND status = $2',
        [quotation_id, 'pending']
      );
      if (quotationCheck.rows.length === 0) {
        await pool.query('ROLLBACK');
        return res.status(404).json({ message: 'Quotation not found or not in pending status' });
      }

      await pool.query(
        'UPDATE public.fwcquotations SET status = $1, updated_at = NOW() WHERE quotation_id = $2',
        ['booked', quotation_id]
      );
    }

    await pool.query('COMMIT');

    res.status(201).json({
      message: 'Booking created successfully',
      id: bookingResult.rows[0].id,
      created_at: bookingResult.rows[0].created_at,
      customer_type: bookingResult.rows[0].customer_type,
      pdf_path: bookingResult.rows[0].pdf,
      order_id: bookingResult.rows[0].order_id,
      quotation_id,
      pdf_url: `/invoice/${order_id}`
    });
  } catch (err) {
    await pool.query('ROLLBACK');
    console.error(`Failed to create booking for order_id ${req.body.order_id}:`, err.message);
    res.status(500).json({ message: 'Failed to create booking', error: err.message });
  }
};

exports.updateBooking = async (req, res) => {
  try {
    const { order_id } = req.params;
    const { products, net_rate, you_save, total, promo_discount, additional_discount, status, transport_details } = req.body;

    if (!order_id || !/^[a-zA-Z0-9-_]+$/.test(order_id)) return res.status(400).json({ message: 'Invalid or missing Order ID' });
    if (products && (!Array.isArray(products) || products.length === 0)) return res.status(400).json({ message: 'Products array is required and must not be empty' });
    if (total && (isNaN(parseFloat(total)) || parseFloat(total) <= 0)) return res.status(400).json({ message: 'Total must be a positive number' });
    if (status && !['booked', 'paid', 'dispatched', 'canceled'].includes(status)) return res.status(400).json({ message: 'Invalid status' });
    if (status === 'dispatched' && !transport_details) return res.status(400).json({ message: 'Transport details required for dispatched status' });

    const parsedNetRate = net_rate !== undefined ? parseFloat(net_rate) : undefined;
    const parsedYouSave = you_save !== undefined ? parseFloat(you_save) : undefined;
    const parsedPromoDiscount = promo_discount !== undefined ? parseFloat(promo_discount) : undefined;
    const parsedAdditionalDiscount = parseFloat(additional_discount) || 0;
    const parsedTotal = parseFloat(total);

    if ([parsedNetRate, parsedYouSave, parsedPromoDiscount, parsedAdditionalDiscount, parsedTotal].some(v => isNaN(v)))
      return res.status(400).json({ message: 'net_rate, you_save, promo_discount, and total must be valid numbers' });

    const bookingCheck = await pool.query(
      'SELECT * FROM public.bookings WHERE order_id = $1',
      [order_id]
    );
    if (bookingCheck.rows.length === 0) return res.status(404).json({ message: 'Booking not found' });

    const booking = bookingCheck.rows[0];
    let customerDetails = {
      customer_name: booking.customer_name,
      address: booking.address,
      mobile_number: booking.mobile_number,
      email: booking.email,
      district: booking.district,
      state: booking.state
    };
    let agent_name = null;

    if (booking.customer_id) {
      const customerCheck = await pool.query(
        'SELECT customer_name, address, mobile_number, email, district, state, customer_type, agent_id FROM public.customers WHERE id = $1',
        [booking.customer_id]
      );
      if (customerCheck.rows.length > 0) {
        customerDetails = customerCheck.rows[0];
        if (customerDetails.customer_type === 'Customer of Selected Agent' && customerDetails.agent_id) {
          const agentCheck = await pool.query('SELECT customer_name FROM public.customers WHERE id = $1', [customerDetails.agent_id]);
          if (agentCheck.rows.length > 0) agent_name = agentCheck.rows[0].customer_name;
        }
      }
    }

    if (products) {
      for (const product of products) {
        const { id, product_type, quantity, price, discount, additional_discount } = product;
        if (!id || !product_type || quantity < 1 || isNaN(parseFloat(price)) || isNaN(parseFloat(discount)) || isNaN(parseFloat(additional_discount)))
          return res.status(400).json({ message: 'Invalid product entry' });

        const tableName = product_type.toLowerCase().replace(/\s+/g, '_');
        const productCheck = await pool.query(`SELECT id FROM public.${tableName} WHERE id = $1`, [id]);
        if (productCheck.rows.length === 0)
          return res.status(404).json({ message: `Product ${id} of type ${product_type} not found or unavailable` });
      }
    }

    let pdfPath = booking.pdf;
    if (products && parsedTotal !== undefined) {
      const pdfResult = await generatePDF(
        'invoice',
        { order_id, customer_type: booking.customer_type, total: parsedTotal, agent_name },
        customerDetails,
        products,
        {
          net_rate: parsedNetRate !== undefined ? parsedNetRate : parseFloat(booking.net_rate || 0),
          you_save: parsedYouSave !== undefined ? parsedYouSave : parseFloat(booking.you_save || 0),
          total: parsedTotal !== undefined ? parsedTotal : parseFloat(booking.total || 0),
          promo_discount: parsedPromoDiscount !== undefined ? parsedPromoDiscount : parseFloat(booking.promo_discount || 0),
          additional_discount: parsedAdditionalDiscount !== undefined ? parsedAdditionalDiscount : parseFloat(booking.additional_discount || 0)
        }
      );
      pdfPath = pdfResult.pdfPath;
    }

    const updateFields = [];
    const updateValues = [];
    let paramIndex = 1;

    if (products) {
      updateFields.push(`products = $${paramIndex++}`);
      updateValues.push(JSON.stringify(products));
    }
    if (parsedNetRate !== undefined) {
      updateFields.push(`net_rate = $${paramIndex++}`);
      updateValues.push(parsedNetRate);
    }
    if (parsedYouSave !== undefined) {
      updateFields.push(`you_save = $${paramIndex++}`);
      updateValues.push(parsedYouSave);
    }
    if (parsedTotal !== undefined) {
      updateFields.push(`total = $${paramIndex++}`);
      updateValues.push(parsedTotal);
    }
    if (parsedPromoDiscount !== undefined) {
      updateFields.push(`promo_discount = $${paramIndex++}`);
      updateValues.push(parsedPromoDiscount);
    }
    if (parsedAdditionalDiscount !== undefined) {
      updateFields.push(`additional_discount = $${paramIndex++}`);
      updateValues.push(parsedAdditionalDiscount);
    }
    if (pdfPath) {
      updateFields.push(`pdf = $${paramIndex++}`);
      updateValues.push(pdfPath);
    }
    if (status) {
      updateFields.push(`status = $${paramIndex++}`);
      updateValues.push(status);
    }
    if (transport_details) {
      updateFields.push(`transport_details = $${paramIndex++}`);
      updateValues.push(JSON.stringify(transport_details));
    }
    updateFields.push(`updated_at = NOW()`);

    if (updateFields.length === 1) {
      return res.status(400).json({ message: 'No fields to update' });
    }

    const query = `
      UPDATE public.bookings 
      SET ${updateFields.join(', ')}
      WHERE order_id = $${paramIndex}
      RETURNING id, order_id, status
    `;
    updateValues.push(order_id);

    const result = await pool.query(query, updateValues);

    res.status(200).json({
      message: 'Booking updated successfully',
      id: result.rows[0].id,
      order_id: result.rows[0].order_id,
      status: result.rows[0].status
    });
  } catch (err) {
    console.error(`Failed to update booking ${req.params.order_id}:`, err.message);
    res.status(500).json({ message: 'Failed to update booking', error: err.message });
  }
};

exports.getInvoice = async (req, res) => {
  try {
    let { order_id } = req.params;
    if (order_id.endsWith('.pdf')) order_id = order_id.replace(/\.pdf$/, '');
    if (!/^[a-zA-Z0-9-_]+$/.test(order_id)) return res.status(400).json({ message: 'Invalid order_id format' });

    const bookingQuery = await pool.query(
      'SELECT products, net_rate, you_save, total, promo_discount, additional_discount, customer_name, address, mobile_number, email, district, state, customer_type, pdf, customer_id, status FROM public.bookings WHERE order_id = $1',
      [order_id]
    );

    if (bookingQuery.rows.length === 0) {
      console.error(`Booking not found for ID: ${order_id}`);
      return res.status(404).json({ message: 'Booking not found' });
    }

    const { products, net_rate, you_save, total, promo_discount, additional_discount, customer_name, address, mobile_number, email, district, state, customer_type, pdf, customer_id, status } = bookingQuery.rows[0];
    let agent_name = null;
    if (customer_type === 'Customer of Selected Agent' && customer_id) {
      const customerCheck = await pool.query('SELECT agent_id FROM public.customers WHERE id = $1', [customer_id]);
      if (customerCheck.rows.length > 0 && customerCheck.rows[0].agent_id) {
        const agentCheck = await pool.query('SELECT customer_name FROM public.customers WHERE id = $1', [customerCheck.rows[0].agent_id]);
        if (agentCheck.rows.length > 0) agent_name = agentCheck.rows[0].customer_name;
      }
    }

    let pdfPath = pdf;
    if (!fs.existsSync(pdf)) {
      console.log(`PDF not found at ${pdf}, regenerating for order ${order_id}`);
      const parsedProducts = typeof products === 'string' ? JSON.parse(products) : products;
      const pdfResult = await generatePDF(
        'invoice',
        { order_id, customer_type, total: parseFloat(total || 0), agent_name },
        { customer_name, address, mobile_number, email, district, state },
        parsedProducts,
        { 
          net_rate: parseFloat(net_rate || 0), 
          you_save: parseFloat(you_save || 0), 
          total: parseFloat(total || 0), 
          promo_discount: parseFloat(promo_discount || 0),
          additional_discount: parseFloat(additional_discount ||0)
        }
      );
      pdfPath = pdfResult.pdfPath;

      await pool.query(
        'UPDATE public.bookings SET pdf = $1 WHERE order_id = $2',
        [pdfPath, order_id]
      );

      try {
        await sendBookingEmail(
          'kidscrackerspark@gmail.com',
          {
            order_id,
            customer_type,
            net_rate: parseFloat(net_rate || 0),
            you_save: parseFloat(you_save || 0),
            total: parseFloat(total || 0)
          },
          { customer_name, address, mobile_number, email, district, state },
          pdfPath,
          parsedProducts,
          'invoice'
        );
      } catch (emailErr) {
        console.error(`Failed to send email during invoice PDF regeneration for ${order_id}:`, emailErr.message);
      }
    }

    if (!fs.existsSync(pdfPath)) {
      console.error(`PDF file not found after generation: ${pdfPath}`);
      return res.status(500).json({ message: 'PDF file not found after generation', error: 'File system error' });
    }

    const safeCustomerName = (customer_name || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename=${safeCustomerName}-${order_id}-invoice.pdf`);
    fs.createReadStream(pdfPath).pipe(res);
  } catch (err) {
    console.error(`Failed to fetch invoice ${req.params.order_id}:`, err.message);
    res.status(500).json({ message: 'Failed to fetch invoice', error: err.message });
  }
};

exports.searchBookings = async (req, res) => {
  try {
    const { customer_name, mobile_number } = req.body;

    if (!customer_name || !mobile_number) {
      return res.status(400).json({ message: 'Customer name and mobile number are required' });
    }

    const query = `
      SELECT id, order_id, quotation_id, products, net_rate, you_save, total, 
             promo_discount, additional_discount, customer_name, address, mobile_number, email, district, state, 
             customer_type, status, created_at, pdf, transport_name, lr_number, transport_contact,
             processing_date, dispatch_date, delivery_date
      FROM public.bookings 
      WHERE LOWER(customer_name) LIKE LOWER($1) 
      AND mobile_number LIKE $2
      ORDER BY created_at DESC
    `;

    const result = await pool.query(query, [`%${customer_name}%`, `%${mobile_number}%`]);
    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Failed to search bookings:', err.message);
    res.status(500).json({ message: 'Failed to search bookings', error: err.message });
  }
};

exports.searchQuotations = async (req, res) => {
  try {
    const { customer_name, mobile_number } = req.body;

    if (!customer_name || !mobile_number) {
      return res.status(400).json({ message: "Customer name and mobile number are required" });
    }

    const query = `
      SELECT id, quotation_id, products, net_rate, you_save, total, 
             promo_discount, additional_discount customer_name, address, mobile_number, email, district, state, 
             customer_type, status, created_at, pdf
      FROM public.fwcquotations
      WHERE LOWER(customer_name) LIKE LOWER($1) 
      AND mobile_number LIKE $2
      ORDER BY created_at DESC
    `;

    const result = await pool.query(query, [`%${customer_name}%`, `%${mobile_number}%`]);
    
    // Add empty transport details to maintain consistent response structure
    const quotations = result.rows.map(row => ({
      ...row,
      type: 'quotation',
      transport_name: null,
      lr_number: null,
      transport_contact: null,
      dispatch_date: null,
      delivery_date: null,
    }));

    res.status(200).json(quotations);
  } catch (err) {
    console.error("Failed to search quotations:", err.message);
    res.status(500).json({ message: "Failed to search quotations", error: err.message });
  }
};