PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS providers (
  Provider_ID INTEGER PRIMARY KEY,
  Name TEXT NOT NULL,
  Type TEXT CHECK (Type IN ('Restaurant','Grocery Store','Supermarket','Bakery','Caterer','Other')) DEFAULT 'Other',
  Address TEXT,
  City TEXT,
  Contact TEXT
);

CREATE TABLE IF NOT EXISTS receivers (
  Receiver_ID INTEGER PRIMARY KEY,
  Name TEXT NOT NULL,
  Type TEXT CHECK (Type IN ('NGO','Community Center','Individual','Shelter','Other')) DEFAULT 'Other',
  City TEXT,
  Contact TEXT
);

CREATE TABLE IF NOT EXISTS food_listings (
  Food_ID INTEGER PRIMARY KEY,
  Food_Name TEXT NOT NULL,
  Quantity INTEGER CHECK (Quantity >= 0),
  Expiry_Date DATE,
  Provider_ID INTEGER NOT NULL,
  Provider_Type TEXT,
  Location TEXT,
  Food_Type TEXT CHECK (Food_Type IN ('Vegetarian','Non-Vegetarian','Vegan','Other')) DEFAULT 'Other',
  Meal_Type TEXT CHECK (Meal_Type IN ('Breakfast','Lunch','Dinner','Snacks','Other')) DEFAULT 'Other',
  FOREIGN KEY (Provider_ID) REFERENCES providers(Provider_ID) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS claims (
  Claim_ID INTEGER PRIMARY KEY,
  Food_ID INTEGER NOT NULL,
  Receiver_ID INTEGER NOT NULL,
  Status TEXT CHECK (Status IN ('Pending','Completed','Cancelled')) DEFAULT 'Pending',
  Timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (Food_ID) REFERENCES food_listings(Food_ID) ON DELETE CASCADE,
  FOREIGN KEY (Receiver_ID) REFERENCES receivers(Receiver_ID) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_food_listings_city ON food_listings(Location);
CREATE INDEX IF NOT EXISTS idx_food_listings_provider ON food_listings(Provider_ID);
CREATE INDEX IF NOT EXISTS idx_claims_food ON claims(Food_ID);
CREATE INDEX IF NOT EXISTS idx_claims_receiver ON claims(Receiver_ID);
