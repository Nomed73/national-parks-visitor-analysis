-- ============================================
-- Schema for tracking major US events that may affect park visitation
-- ============================================

-- Table: event_types
-- Purpose: Lookup table for high-level event categories (e.g., pandemic, hurricane, wildfire)
CREATE TABLE IF NOT EXISTS event_types (
    id SERIAL PRIMARY KEY,
    event_type TEXT UNIQUE NOT NULL
);

-- Table: major_us_events
-- Purpose: Stores specific events with time range, impact, and metadata
CREATE TABLE IF NOT EXISTS major_us_events (
    id SERIAL PRIMARY KEY,
    event_name TEXT NOT NULL,                  -- e.g., "COVID-19 Pandemic", "Hurricane Katrina"
    event_type_id INT REFERENCES event_types(id),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    year INT,                                  -- For easier joins to monthly park data
    month INT,                                 -- For easier joins to monthly park data
    impact_level TEXT,                         -- e.g., 'national', 'multi-state', 'state', 'local'
    description TEXT,
    source_url TEXT
);

-- Table: event_locations
-- Purpose: Associates each event with affected states and/or regions
-- Allows flexible handling of multi-state/national events
CREATE TABLE IF NOT EXISTS event_locations (
    id SERIAL PRIMARY KEY,
    event_id INT REFERENCES major_us_events(id) ON DELETE CASCADE,
    state TEXT,                                -- State abbreviation (nullable if only region used)
    region TEXT                                 -- Optional region info (e.g., "Southwest")
);