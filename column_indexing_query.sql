
CREATE INDEX idx_company_date ON stock_prices (company, date);
CREATE INDEX idx_company_open ON stock_prices (company, open);
CREATE INDEX idx_company_close ON stock_prices (company, close);
CREATE INDEX idx_company_volume ON stock_prices (company, volume);

