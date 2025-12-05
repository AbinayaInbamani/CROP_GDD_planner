import os
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from requests.exceptions import HTTPError, RequestException
import streamlit as st
from opencage.geocoder import OpenCageGeocode

# -----------------------------
# 0. Geocoding (OpenCage)
# -----------------------------
def geocode_place(place_name):
    api_key = os.environ.get("OPENCAGE_API_KEY")
    if not api_key:
        raise ValueError("OPENCAGE_API_KEY not set. Configure it in environment or Streamlit secrets.")

    geocoder = OpenCageGeocode(api_key)
    results = geocoder.geocode(place_name, limit=1, no_annotations=1)

    if not results:
        raise ValueError(f"Place not found: {place_name}")

    best = results[0]
    lat = best["geometry"]["lat"]
    lon = best["geometry"]["lng"]
    formatted = best.get("formatted", place_name)
    return lat, lon, formatted

# -----------------------------
# 1. NASA POWER block fetch
# -----------------------------
def get_power_daily_tmax_tmin(lat, lon, start_date, end_date, max_retries=3):
    """Fetch daily T2M_MAX and T2M_MIN from NASA POWER between start_date and end_date.
    Retries a few times if a 5xx error occurs.
    """
    start_str = pd.to_datetime(start_date).strftime("%Y%m%d")
    end_str = pd.to_datetime(end_date).strftime("%Y%m%d")

    base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    params = {
        "start": start_str,
        "end": end_str,
        "latitude": lat,
        "longitude": lon,
        "community": "AG",
        "parameters": "T2M_MAX,T2M_MIN",
        "format": "JSON"
    }

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(base_url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            break
        except HTTPError:
            if 500 <= r.status_code < 600 and attempt < max_retries:
                st.warning(f"NASA POWER server error ({r.status_code}), retry {attempt}/{max_retries}...")
                continue
            else:
                raise
        except RequestException:
            if attempt < max_retries:
                st.warning(f"Network error, retry {attempt}/{max_retries}...")
                continue
            else:
                raise

    param_data = data["properties"]["parameter"]
    tmax_dict = param_data["T2M_MAX"]
    tmin_dict = param_data["T2M_MIN"]

    rows = []
    for d_str in sorted(tmax_dict.keys()):
        date_obj = datetime.strptime(d_str, "%Y%m%d").date()
        rows.append({
            "date": date_obj,
            "T2M_MAX": tmax_dict[d_str],
            "T2M_MIN": tmin_dict[d_str]
        })

    return pd.DataFrame(rows)

# -----------------------------
# 2. GDD functions
# -----------------------------
def daily_gdd(tmax, tmin, tbase):
    tmean = (tmax + tmin) / 2.0
    return max(tmean - tbase, 0.0)

def simulate_gdd(lat, lon, start_date, tbase,
                 targets=(100, 300, 500, 1000),
                 max_days=365*3,
                 block_days=30):
    """Fetches NASA POWER data in small blocks (e.g. 30 days) and
    stops when highest target reached or max_days passed.
    """
    start_date = pd.to_datetime(start_date).date()
    end_date_limit = start_date + timedelta(days=max_days)

    cum_gdd = 0.0
    stage_dates = {thr: None for thr in targets}
    history_rows = []

    current_start = start_date
    highest_target = max(targets)

    while cum_gdd < highest_target and current_start <= end_date_limit:
        current_end = min(current_start + timedelta(days=block_days - 1), end_date_limit)
        st.write(f"Fetching NASA POWER data: {current_start} to {current_end}")

        try:
            df_block = get_power_daily_tmax_tmin(lat, lon, current_start, current_end)
        except Exception:
            st.error(f"Stopped: could not get data for {current_start}–{current_end}.")
            st.info("Error from NASA POWER or network. Try a different date range or later.")
            break

        for _, row in df_block.iterrows():
            if cum_gdd >= highest_target:
                break

            date_d = row["date"]
            tmax = row["T2M_MAX"]
            tmin = row["T2M_MIN"]
            gdd_day = daily_gdd(tmax, tmin, tbase)
            cum_gdd += gdd_day

            history_rows.append({
                "date": date_d,
                "T2M_MAX": tmax,
                "T2M_MIN": tmin,
                "GDD_day": gdd_day,
                "GDD_cum": cum_gdd
            })

            for thr in targets:
                if stage_dates[thr] is None and cum_gdd >= thr:
                    stage_dates[thr] = date_d

        current_start = current_end + timedelta(days=1)

    return pd.DataFrame(history_rows), stage_dates

# -----------------------------
# 3. Streamlit UI
# -----------------------------
def main():
    st.title("Farmer-friendly GDD tracker (NASA POWER)")

    st.markdown(
        "This tool calculates Growing Degree Days (GDD) from NASA POWER daily Tmax/Tmin "
        "for your village/town and predicts when cumulative GDD will reach key stages."
    )

    place_name = st.text_input("Place name (village/town/district, etc.)", value="Chennai, India")
    start_date_str = st.text_input("Start date (YYYY-MM-DD)", value="2025-01-01")
    tbase = st.number_input("Base temperature (°C)", value=10.0)

    use_manual_coords = st.checkbox("I know my latitude/longitude (skip geocoding)")

    targets_default = (100, 300, 500, 1000)
    if st.checkbox("Customize GDD targets", value=False):
        targets_input = st.text_input(
            "Enter comma-separated GDD targets",
            value="100,300,500,1000"
        )
        try:
            targets = tuple(sorted({int(x.strip()) for x in targets_input.split(",") if x.strip()}))
        except ValueError:
            st.error("Targets must be integers separated by commas. Using defaults.")
            targets = targets_default
    else:
        targets = targets_default

    if st.button("Run GDD simulation"):
        if use_manual_coords:
            lat = st.number_input("Latitude", value=13.0827)
            lon = st.number_input("Longitude", value=80.2707)
            full_address = f"Manual coordinates: {lat}, {lon}"
        else:
            if not place_name:
                st.error("Please enter a place name.")
                return
            try:
                st.write("Geocoding your place with OpenCage...")
                lat, lon, full_address = geocode_place(place_name)
            except Exception as e:
                st.error(f"Could not geocode that place: {e}")
                return

        st.success(f"Location: {full_address}")
        st.write(f"Latitude: {lat:.4f}, Longitude: {lon:.4f}")
        st.write(f"Start date: {start_date_str}")
        st.write(f"Base temperature: {tbase} °C")

        history_df, stage_dates = simulate_gdd(
            lat=lat,
            lon=lon,
            start_date=start_date_str,
            tbase=tbase,
            targets=targets
        )

        if history_df.empty:
            st.warning("No GDD data could be calculated (NASA POWER did not return data).")
            return

        st.subheader("Predicted stages (if reached)")
        labels = {
            100: "Blowing date (100 GDD)",
            300: "Sprout (300 GDD)",
            500: "Bloom (500 GDD)",
            1000: "Colour change / Harvest (1000 GDD)"
        }
        for thr in targets:
            label = labels.get(thr, f"{thr} GDD")
            st.write(f"- {label}: {stage_dates[thr]}")

        st.subheader("Last 10 days of GDD history")
        st.dataframe(history_df.tail(10))

        st.subheader("Cumulative GDD over time")
        plot_df = history_df.copy()
        plot_df = plot_df.set_index("date")
        st.line_chart(plot_df["GDD_cum"])

        with st.expander("Show full GDD table"):
            st.dataframe(history_df)

if __name__ == "__main__":
    main()
