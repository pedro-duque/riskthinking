import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.decomposition import PCA
from sklearn.utils import shuffle
from sklearn.preprocessing import StandardScaler

def mutate_integrate_ml(data):
    # Assume `data` is loaded as a Pandas DataFrame
    data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date', inplace=True)
    
    # Remove rows with NaN values
    data.dropna(inplace=True)
    
    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = data[features]
    y = data[target]

    # Perform dimensionality reduction with PCA
    pca = PCA(n_components=0.95)
    X = pca.fit_transform(X)
    
    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Create a memory-efficient RandomForestRegressor model
    model = RandomForestRegressor(n_estimators=50, max_depth=5, random_state=42)
    
    # Initialize data loss metric
    total_data = len(X_train)
    data_loss = 0

    # Iterate over mini-batches and perform partial fitting
    batch_size = 1000
    num_iterations = len(X_train) // batch_size

    # Train the model
    for i in range(num_iterations):
        start_idx = i * batch_size
        end_idx = (i + 1) * batch_size

        X_batch, y_batch = X_train[start_idx:end_idx], y_train[start_idx:end_idx]

        # Shuffle the data within each mini-batch
        X_batch, y_batch = shuffle(X_batch, y_batch, random_state=42)

        # Fit the model on the mini-batch
        model.fit(X_batch, y_batch)

        # Calculate data loss
        data_loss += len(X_batch)
    
    # Calculate percentage of data loss
    data_loss_percentage = (1 - data_loss / total_data) * 100
    
    # Make predictions on test data
    y_pred = model.predict(X_test)
    
    
    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = mean_absolute_error(y_test, y_pred)
    
    mse = mean_squared_error(y_test, y_pred)

    return model, {"Mean Absolute Error": mae, "Mean Squared Error": mse, "Data Loss Percentage": data_loss_percentage}