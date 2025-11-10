import os
import pandas as pd
import streamlit as st
from pymongo import MongoClient
import plotly.express as px
import plotly.graph_objects as go

# ==============================
# 🎯 Streamlit Page Configuration
# ==============================
st.set_page_config(
    page_title="🎬 MFlix Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom styling
st.markdown("""
<style>
    .metric-card { background-color: #f0f2f6; padding: 15px; border-radius: 8px; }
    .title-main { font-size: 2.5rem; font-weight: bold; color: #1f77b4; }
</style>
""", unsafe_allow_html=True)

st.title("🎬 MFlix Analytics Dashboard")
st.caption("📊 Real-time analytics dashboard powered by MongoDB Atlas and Streamlit")

# ==============================
# 🔑 Database Connection Setup
# ==============================
MONGO_URI = st.secrets.get("MONGO_URI", os.getenv("MONGO_URI", ""))
DB_NAME = st.secrets.get("DB_NAME", os.getenv("DB_NAME", "sample_mflix"))

if not MONGO_URI:
    st.error("❌ MongoDB connection string missing. Please add MONGO_URI to .streamlit/secrets.toml")
    st.stop()

@st.cache_resource
def get_db():
    """Establish and cache MongoDB connection"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Verify connection
        client.admin.command("ping")
        return client[DB_NAME]
    except Exception as e:
        st.error(f"❌ Database connection failed: {str(e)}")
        return None

db = get_db()
if db is None:
    st.stop()

#st.success(f"✅ Connected to MongoDB: {DB_NAME}")

# ==============================
# 📊 Key Performance Indicators
# ==============================
st.subheader("📈 Key Performance Indicators (KPIs)")

# Calculate metrics
try:
    movies_count = db.movies.count_documents({})
    users_count = db.users.count_documents({}) if "users" in db.list_collection_names() else 0
    comments_count = db.comments.count_documents({}) if "comments" in db.list_collection_names() else 0
    
    # Get average rating
    rating_data = list(db.movies.aggregate([
        {"$match": {"imdb.rating": {"$type": "number"}}},
        {"$group": {"_id": None, "avgRating": {"$avg": "$imdb.rating"}}}
    ]))
    avg_rating = rating_data[0]["avgRating"] if rating_data else 0
    
except Exception as e:
    st.error(f"Error calculating metrics: {e}")
    movies_count = users_count = comments_count = avg_rating = 0

# Display KPIs
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("🎞️ Total Movies", f"{movies_count:,}")
with col2:
    st.metric("👥 Registered Users", f"{users_count:,}")
with col3:
    st.metric("💬 Comments", f"{comments_count:,}")
with col4:
    st.metric("⭐ Avg IMDb Rating", f"{avg_rating:.2f}/10")

st.divider()

# ==============================
# ⚙️ Sidebar Filters
# ==============================
with st.sidebar:
    st.header("🔍 Filters & Controls")
    st.write("Customize your analysis below:")
    
    # Year range filter
    try:
        year_stats = list(db.movies.aggregate([
            {"$match": {"year": {"$type": "number"}}},
            {"$group": {"_id": None, "minYear": {"$min": "$year"}, "maxYear": {"$max": "$year"}}}
        ]))
        min_year = int(year_stats[0]["minYear"]) if year_stats else 1900
        max_year = int(year_stats[0]["maxYear"]) if year_stats else 2025
    except:
        min_year, max_year = 1900, 2025
    
    year_range = st.slider(
        "📅 Release Year Range",
        min_value=min_year,
        max_value=max_year,
        value=(max(min_year, 1980), max_year),
        step=1
    )
    
    # Genre filter
    try:
        genres_data = list(db.movies.aggregate([
            {"$unwind": "$genres"},
            {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 50}
        ]))
        genre_list = [g["_id"] for g in genres_data if g.get("_id")]
    except:
        genre_list = []
    
    selected_genres = st.multiselect(
        "🎭 Filter by Genres",
        options=genre_list,
        default=[]
    )
    
    # IMDb rating filter
    rating_filter = st.slider(
        "⭐ Minimum IMDb Rating",
        min_value=0.0,
        max_value=10.0,
        value=5.0,
        step=0.5
    )

# Build match stage for filters
match_stage = {
    "year": {"$gte": year_range[0], "$lte": year_range[1]},
    "imdb.rating": {"$gte": rating_filter, "$type": "number"}
}

if selected_genres:
    match_stage["genres"] = {"$in": selected_genres}

# ==============================
# 📈 Rating Trend Over Time
# ==============================
st.subheader("📉 Average Rating Trend by Release Year")

try:
    pipeline = [
        {"$match": {**match_stage}},
        {"$group": {
            "_id": "$year",
            "avgRating": {"$avg": "$imdb.rating"},
            "movieCount": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    
    df_years = pd.DataFrame(list(db.movies.aggregate(pipeline)))
    
    if not df_years.empty:
        df_years.rename(columns={"_id": "Year"}, inplace=True)
        
        fig_trend = px.line(
            df_years,
            x="Year",
            y="avgRating",
            hover_data={"movieCount": True},
            markers=True,
            title="Average IMDb Rating Trend",
            labels={"avgRating": "Average Rating"}
        )
        fig_trend.update_layout(
            hovermode="x unified",
            yaxis_range=[0, 10],
            height=400
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    else:
        st.info("No data available for the selected filters.")
except Exception as e:
    st.error(f"Error generating trend chart: {e}")

st.divider()

# ==============================
# 🎭 Genre Performance Analysis
# ==============================
st.subheader("🎭 Genre Performance Analysis")

try:
    pipeline = [
        {"$match": match_stage},
        {"$unwind": "$genres"},
        {"$group": {
            "_id": "$genres",
            "avgRating": {"$avg": "$imdb.rating"},
            "movieCount": {"$sum": 1}
        }},
        {"$match": {"_id": {"$ne": None}}},
        {"$sort": {"movieCount": -1}}
    ]
    
    df_genres = pd.DataFrame(list(db.movies.aggregate(pipeline)))
    
    if not df_genres.empty:
        df_genres.rename(columns={"_id": "Genre"}, inplace=True)
        
        # Show metrics table
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Genre Statistics Table**")
            st.dataframe(
                df_genres.head(15).sort_values("movieCount", ascending=False),
                use_container_width=True,
                hide_index=True
            )
        
        with col2:
            # Top genres by rating
            fig_rating = px.bar(
                df_genres.head(15).sort_values("avgRating", ascending=True),
                y="Genre",
                x="avgRating",
                orientation="h",
                title="Top Genres by Average Rating",
                color="avgRating",
                color_continuous_scale="Viridis"
            )
            st.plotly_chart(fig_rating, use_container_width=True)
        
        # Movie count by genre
        fig_count = px.bar(
            df_genres.head(15).sort_values("movieCount", ascending=True),
            y="Genre",
            x="movieCount",
            orientation="h",
            title="Top Genres by Movie Count",
            color="movieCount",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig_count, use_container_width=True)
    else:
        st.warning("No genre data available for current filters.")
except Exception as e:
    st.error(f"Error generating genre analysis: {e}")

st.divider()

# ==============================
# 💬 Most Discussed Movies
# ==============================
if comments_count > 0:
    st.subheader("💬 Most Discussed Movies")
    
    try:
        pipeline = [
            {"$lookup": {
                "from": "comments",
                "localField": "_id",
                "foreignField": "movie_id",
                "as": "all_comments"
            }},
            {"$addFields": {"comment_count": {"$size": "$all_comments"}}},
            {"$match": match_stage},
            {"$sort": {"comment_count": -1}},
            {"$limit": 15},
            {"$project": {
                "title": 1,
                "year": 1,
                "imdb.rating": 1,
                "comment_count": 1,
                "genres": 1
            }}
        ]
        
        df_discussed = pd.DataFrame(list(db.movies.aggregate(pipeline)))
        
        if not df_discussed.empty:
            df_discussed.rename(columns={"imdb.rating": "IMDb Rating"}, inplace=True)
            
            fig_discussed = px.bar(
                df_discussed.head(10),
                x="comment_count",
                y="title",
                orientation="h",
                title="Top Movies by Comment Count",
                labels={"comment_count": "Number of Comments", "title": "Movie Title"},
                color="comment_count",
                color_continuous_scale="Reds"
            )
            fig_discussed.update_layout(yaxis={"categoryorder": "total ascending"})
            
            st.plotly_chart(fig_discussed, use_container_width=True)
            
            st.write("**Detailed Comments Table**")
            st.dataframe(df_discussed, use_container_width=True, hide_index=True)
        else:
            st.info("No comment data available for selected filters.")
    except Exception as e:
        st.error(f"Error generating comments analysis: {e}")

st.divider()

# ==============================
# 🔍 Movie Search Interface
# ==============================
st.subheader("🔍 Quick Movie Search")

search_query = st.text_input("Search movie titles (case-insensitive)...")
result_limit = st.slider("Number of results to display", 5, 100, 20)

if search_query:
    try:
        cursor = db.movies.find(
            {
                "title": {"$regex": search_query, "$options": "i"},
                **match_stage
            },
            {"title": 1, "year": 1, "genres": 1, "imdb.rating": 1, "plot": 1}
        ).limit(result_limit)
        
        df_search = pd.DataFrame(list(cursor))
        
        if not df_search.empty:
            df_search.rename(columns={"imdb.rating": "IMDb Rating"}, inplace=True)
            display_cols = [col for col in ["title", "year", "genres", "IMDb Rating", "plot"] if col in df_search.columns]
            
            st.dataframe(
                df_search[display_cols].drop(columns=["_id"], errors="ignore"),
                use_container_width=True,
                hide_index=True
            )
            st.success(f"✅ Found {len(df_search)} movies matching your search.")
        else:
            st.warning("❌ No movies found matching your search criteria.")
    except Exception as e:
        st.error(f"Search error: {e}")

st.divider()

# ==============================
# 📝 Footer with Information
# ==============================
st.markdown("""
---
**Dashboard Information:**
- **Data Source:** MongoDB Atlas (Cloud-hosted)
- **Database:** sample_mflix
- **Technology Stack:** Python, Streamlit, MongoDB, Plotly
- **Last Updated:** Real-time connection to live database
- **Built By :** Jinank Thakker U00365065 DSA 508 Test 2
""")

