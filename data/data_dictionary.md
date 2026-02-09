# Data Dictionary

## listings.csv

| Column | Type | Description |
|--------|------|-------------|
| id | integer | Unique listing identifier |
| name | string | Listing title |
| host_id | integer | Host identifier |
| host_name | string | Host display name |
| neighbourhood_group | string | Broader area (e.g. ward) |
| neighbourhood | string | Neighborhood name |
| latitude | float | Latitude coordinate |
| longitude | float | Longitude coordinate |
| room_type | string | Entire home/apt, Private room, or Shared room |
| price | string | Nightly price (may include $, commas; can be empty) |
| minimum_nights | integer | Minimum stay in nights |
| number_of_reviews | integer | Total review count |
| last_review | date | Date of most recent review |
| reviews_per_month | float | Average reviews per month |
| calculated_host_listings_count | integer | Number of listings by this host |
| availability_365 | integer | Days available in next 365 days (0-365) |
| number_of_reviews_ltm | integer | Reviews in last 12 months |
| license | string | License identifier if applicable |

## reviews.csv

| Column | Type | Description |
|--------|------|-------------|
| listing_id | integer | Foreign key to listings.id |
| date | date | Review date |
