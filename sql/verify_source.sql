SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM source.customers
UNION ALL SELECT 'products', COUNT(*) FROM source.products
UNION ALL SELECT 'orders', COUNT(*) FROM source.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM source.order_items
UNION ALL SELECT 'payments', COUNT(*) FROM source.payments
ORDER BY table_name;
