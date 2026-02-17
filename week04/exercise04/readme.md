# Data Engineering - Assignment 04
## US Top Music Schools Impacted by Severe Winter Weather (January 2026)

**Author:** Greg Sullivan  
**Date:** February 2026  
**Course:** DATA 5035 - Data Engineering  
**Professor:** Paul Boal

---

## Objective

Build a data pipeline that combines music conservatory data (from HTML scraping and APIs) with weather data (from API) to estimate student-days impacted by severe winter weather in January 2026.

---

## Selected Grouping

**Top 10 Music Schools in the United States**

Source: [www.thebestschools.org](http://www.thebestschools.org/) - "The Best Music Schools" ranking

### Schools Analyzed
1. The Juilliard School (New York, NY)
2. Curtis Institute of Music (Philadelphia, PA)
3. Berklee College of Music (Boston, MA)
4. USC Thornton School of Music (Los Angeles, CA)
5. Oberlin Conservatory of Music (Oberlin, OH)
6. New England Conservatory of Music (Boston, MA)
7. Manhattan School of Music (New York, NY)
8. Indiana University Jacobs School of Music (Bloomington, IN)
9. Eastman School of Music (Rochester, NY)
10. San Francisco Conservatory of Music (San Francisco, CA)

---

## Severe Weather Definition

A day is considered "severe winter weather" if it meets **ANY** of the following criteria:

| Condition | Threshold | Rationale |
|-----------|-----------|-----------|
| Extreme Cold | Minimum temp < 20°F | Dangerous wind chill, potential campus closures |
| Full Freeze | Maximum temp < 32°F | Entire day below freezing, icy conditions |
| Winter Precipitation | Precip > 0.5" while temp < 32°F | Snow/ice accumulation event |

*Note: This reflects my Midwestern perspective on severe winter weather. Regional definitions may vary.*

---

## Execution Environment

### Challenge: Snowflake Network Restrictions

The Snowflake cloud notebook environment has firewall restrictions that block:
- External web scraping requests to school websites
- Some API calls (rate limiting and access restrictions)

### Solution: Hybrid Local + Cloud Approach

To demonstrate full pipeline functionality while working within platform constraints:

1. **Local Execution (Jupyter Notebook):** Ran the data collection pipeline locally where full network access was available
2. **CSV Export:** Exported the results to `music_schools_weather_results.csv`
3. **Cloud Import (Snowflake):** Imported the CSV into Snowflake for SQL analysis and table storage

This hybrid approach allowed the assignment to demonstrate:
- Working web scraping code
- Working API integration
- Proper fallback handling
- SQL analysis in Snowflake

---

## Data Architecture

### Design Principle: Separation of Data and Code

Following software engineering best practices, **all reference data is externalized into a CSV file** rather than hardcoded in the Python code. This approach:

- Allows data updates without code changes
- Enables non-developers to maintain reference data
- Simplifies testing with alternate datasets
- Keeps version control clean (data changes separate from logic changes)
- Improves code reusability

### Reference Data File

| File | Purpose | Format |
|------|---------|--------|
| `music_schools_reference.csv` | Fallback data for all schools | CSV |

### Data Collection Strategy

Data is collected in priority order, with the reference file serving as the verified fallback:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────────┐
│  Web Scraping   │ ──▶ │   API Lookup    │ ──▶ │  Reference CSV Fallback │
│  (Priority 1)   │     │  (Priority 2)   │     │      (Priority 3)       │
└─────────────────┘     └─────────────────┘     └─────────────────────────┘
```

---

## Data Collection Results (Local Run)

Keep in mind, these may not be your results on any given run

### Summary

| Data | Primary Source | Fallback | Actual Result |
|------|---------------|----------|---------------|
| School Names | Web scraping | Reference CSV | ~8/10 scraped |
| Enrollment | Web scraping → API | Reference CSV | **7/10 scraped, 2/10 API, 1/10 fallback** |
| Coordinates | Reference CSV | N/A | 10/10 |
| Weather | Open-Meteo API | N/A | 10/10 |

### Enrollment Data Sources

| School | Method | Enrollment |
|--------|--------|------------|
| Curtis Institute of Music | **Scraped** | 160 |
| Oberlin Conservatory of Music | **Scraped** | 540 |
| New England Conservatory of Music | **Scraped** | 850 |
| Manhattan School of Music | **Scraped** | 960 |
| Indiana University Jacobs School of Music | **Scraped** | 1,500 |
| Eastman School of Music | **Scraped** | 260 |
| San Francisco Conservatory of Music | **Scraped** | 460 |
| The Juilliard School | **API** | 460 |
| Berklee College of Music | **API** | 7,510 |
| USC Thornton School of Music | Fallback | 1,069 |

*Note: USC Thornton required fallback because the College Scorecard API returns full university enrollment, not music school only.*

---

## Final Results

### Impact by School (January 2026)

| School | State | Enrollment | Severe Days | Student-Days Impacted |
|--------|-------|------------|-------------|----------------------|
| Berklee College of Music | MA | 7,943 | 17 | 135,031 |
| Indiana University Jacobs School of Music | IN | 500 | 15 | 7,500 |
| New England Conservatory of Music | MA | 850 | 17 | 14,450 |
| Manhattan School of Music | NY | 1,070 | 15 | 16,050 |
| Oberlin Conservatory of Music | OH | 540 | 23 | 12,420 |
| The Juilliard School | NY | 1,200 | 15 | 18,000 |
| Eastman School of Music | NY | 950 | 20 | 15,000 |
| Curtis Institute of Music | PA | 160 | 13 | 2,080 |
| USC Thornton School of Music | CA | 1,069 | 0 | 0 |
| San Francisco Conservatory of Music | CA | 215 | 0 | 0 |

**Total Student-Days Impacted: 205,620**

### Key Insights

1. **Berklee dominates impact** - 62% of all student-days due to large enrollment (7,943) + Boston's severe weather (17 days)
2. **Oberlin was the harshest location** - 23 severe days (74% of January) in rural Ohio
3. **California escaped entirely** - Both LA and SF had zero severe weather days
4. **Rochester surprised** - 20 severe days, but large enrollment (950) raised total impact
5. **New York City schools** - 15 severe days affected 3 schools (Juilliard and Manhattan)

---

## Stretch Work Completed

### 1. Year-over-Year Comparison (January 2026 vs 2025)
- Fetched January 2025 weather data for baseline comparison
- Applied identical severe weather criteria
- Calculated difference and assessment for each school

### 2. Severity Index (0-100 scale)
Created a weighted severity score based on:

| Component | Max Points | Logic |
|-----------|-----------|-------|
| Cold Intensity | 40 | How far temps dropped below 20°F (2 pts/degree) |
| Duration | 30 | Longest consecutive severe day streak (3 pts/day) |
| Precipitation | 30 | Total inches during freezing temps (10 pts/inch) |

**Severity Categories:**
- Extreme: 60-100
- Severe: 40-59
- Moderate: 20-39
- Mild: 1-19
- None: 0

### 3. Visualizations
Five visualizations were created:
1. **Student-Days Impacted** - Horizontal bar chart showing total impact by school
2. **Severe Weather Days** - Horizontal bar chart showing raw weather severity by location
3. **Enrollment vs Severity Scatter Plot** - Bubble chart (bubble size = impact)
4. **Year-over-Year Comparison** - Grouped bar chart comparing 2025 vs 2026
5. **Severity Index** - Horizontal bar chart with color-coded severity categories

---

## Data Engineering Challenges Encountered

| Challenge | Description | Solution |
|-----------|-------------|----------|
| Network Restrictions | Snowflake firewall blocked external web requests | Ran pipeline locally, imported CSV |
| API Rate Limits | College Scorecard API limits requests | Added delays between calls |
| Inconsistent HTML | Each school website has unique structure | Custom scraping patterns per site |
| Data in PDFs | Berklee publishes enrollment in PDF Factbook | Used API as secondary source |
| University vs School | API returns full university enrollment | Used fallback for USC Thornton |

### Key Lessons Learned

> "A simple Google search shows enrollment data instantly, but programmatic access requires navigating rate limits, firewalls, and inconsistent data formats." 
> "Now I understand why data engineers say '80% of the work is getting the data.'"
> "If you are going to play tuba, trombone or cello, you should go to music school in California!"

---

## Technology Stack

- **Platforms:** 
  - Jupyter Notebook (local data collection)
  - Snowflake Notebooks (SQL analysis and storage)
- **Languages:** Python, SQL
- **Libraries:** 
  - `requests` - HTTP requests to APIs
  - `pandas` - Data manipulation
  - `BeautifulSoup` - HTML parsing
  - `matplotlib` - Visualizations
  - `re` - Regular expressions
- **Database:** Snowflake (SNOWBEARAIR_DB.PUBLIC)
- **APIs:**
  - Open-Meteo Historical Weather API
  - College Scorecard API (data.gov)

---

## Files

| File | Description |
|------|-------------|
| `exercise04.ipynb` | Snowflake notebook (SQL analysis, visualizations) |
| `readme.md` | This documentation file |
| `music_schools_reference.csv` | Reference/fallback data file |
| `music_schools_weather_results.csv` | Exported results from local run |

---

## How to Reproduce

### Snowflake Analysis
1. Upload `music_schools_weather_results.csv` to Snowflake
2. Load into table `SNOWBEARAIR_DB.PUBLIC.MUSIC_SCHOOLS_WINTER_WEATHER_IMPACT_JAN2026`
3. Open `exercise04.ipynb` in Snowflake Notebooks
4. Run SQL analysis and visualization cells

---

## Output Table

`SNOWBEARAIR_DB.PUBLIC.MUSIC_SCHOOLS_WINTER_WEATHER_IMPACT_JAN2026`

| Column | Type | Description |
|--------|------|-------------|
| SCHOOL_NAME | VARCHAR | Official school name |
| CITY | VARCHAR | School city |
| STATE | VARCHAR | School state |
| URL | VARCHAR | School website |
| ENROLLMENT | NUMBER | Student enrollment |
| SEVERE_WEATHER_DAYS_COUNT | NUMBER | Days meeting severe weather criteria |
| SEVERE_WEATHER_DATES | VARCHAR | Comma-separated list of dates |
| STUDENT_DAYS_IMPACTED | NUMBER | Enrollment × Severe Days |

---

## References

- [The Best Schools - Top Music Schools](http://www.thebestschools.org/)
- [Open-Meteo Historical Weather API](https://open-meteo.com/)
- [College Scorecard API](https://collegescorecard.ed.gov/data/)
- Common Data Sets (2023-2024) for enrollment fallback data
