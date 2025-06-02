from sklearn.ensemble import IsolationForest

def detect_anomalies(df):
    model = IsolationForest(contamination=0.01)
    features = df[['amount']]  # Add more features for better results
    df['anomaly'] = model.fit_predict(features)
    return df
