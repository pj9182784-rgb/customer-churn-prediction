import streamlit as st
import pandas as pd
import joblib
MODEL_PATH = "D:\\project2\\code\\vw_churn_training_lr.pkl"
model = joblib.load(MODEL_PATH)
st.title("customer churn Predictor")

event1=st.number_input("hist_recency_days", min_value=0, value=0)
event2=st.number_input("hist_total_orders", min_value=0, value=0)
event3=st.number_input("hist_total_revenue", min_value=0, value=0)
event4=st.number_input("mh_clicks", min_value=0, value=0)
event5=st.number_input("mh_opens", min_value=0, value=0)
event6=st.number_input("mh_click_rate", min_value=0, value=0)
event7=st.number_input("th_total_tickets", min_value=0, value=0)
event8=st.number_input("th_resolved_tickets", min_value=0, value=0)
event9=st.number_input("th_closed_tickets", min_value=0, value=0)
event10=st.number_input("loyalty_tier", min_value=0, value=0)




if st.button("Predict"):
    input_df = pd.DataFrame([{
       'hist_recency_days':event1, 
       'hist_total_orders':event2,
       'hist_total_revenue':event3,
       'mh_clicks':event4, 
       'mh_opens':event5, 
       'mh_click_rate':event6,
       'th_total_tickets':event7,
       'th_resolved_tickets':event8, 
       'th_closed_tickets':event9,
       'loyalty_tier':event10,
       }])
    
    pred= model.predict(input_df)[0]
    st.success(f"Prediction: {'Churn' if pred == 1 else 'Not Churn'}")