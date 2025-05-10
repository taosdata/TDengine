# encoding:utf-8
"""train the model for anomaly detection"""
import joblib
import keras
import numpy as np
import pandas as pd
from keras.api import layers

from matplotlib import pyplot as plt
from taosanalytics.util import create_sequences


def get_training_data():
    """ load the remote training data """
    url_str = ("https://raw.githubusercontent.com/numenta/NAB/master/data/artificialNoAnomaly/"
               "art_daily_small_noise.csv")
    df_small_noise = pd.read_csv(url_str, parse_dates=True, index_col="timestamp")
    return df_small_noise


def do_train_model():
    """ do train the model by using input data """
    df_small_noise = get_training_data()
    time_steps = 288

    training_mean = df_small_noise.mean()
    training_std = df_small_noise.std()

    info = {
        "mean": training_mean,
        "std": training_std,
        "timesteps": time_steps,
    }

    df_training_value = (df_small_noise - training_mean) / training_std
    print("Number of training samples:", len(df_training_value))

    x_train = create_sequences(df_training_value.values, time_steps)
    print("Training input shape: ", x_train.shape)

    model = keras.Sequential(
        [
            layers.Input(shape=(x_train.shape[1], x_train.shape[2])),
            layers.Conv1D(
                filters=32,
                kernel_size=7,
                padding="same",
                strides=2,
                activation="relu",
            ),
            layers.Dropout(rate=0.2),
            layers.Conv1D(
                filters=16,
                kernel_size=7,
                padding="same",
                strides=2,
                activation="relu",
            ),
            layers.Conv1DTranspose(
                filters=16,
                kernel_size=7,
                padding="same",
                strides=2,
                activation="relu",
            ),
            layers.Dropout(rate=0.2),
            layers.Conv1DTranspose(
                filters=32,
                kernel_size=7,
                padding="same",
                strides=2,
                activation="relu",
            ),
            layers.Conv1DTranspose(filters=1, kernel_size=7, padding="same"),
        ]
    )

    model.compile(optimizer=keras.optimizers.Adam(learning_rate=0.001), loss="mse")
    model.summary()

    history = model.fit(
        x_train,
        x_train,
        epochs=50,
        batch_size=128,
        validation_split=0.1,
        callbacks=[
            keras.callbacks.EarlyStopping(monitor="val_loss", patience=5, mode="min")
        ],
    )

    print(history)

    # Get train MAE loss.
    x_train_pred = model.predict(x_train)
    train_mae_loss = np.mean(np.abs(x_train_pred - x_train), axis=1)

    plt.hist(train_mae_loss, bins=50)
    plt.xlabel("Train MAE loss")
    plt.ylabel("No of samples")
    plt.show()

    # Get reconstruction loss threshold.
    threshold = np.max(train_mae_loss)
    print("Reconstruction error threshold: ", threshold)

    model.save('../../model/sample-ad-autoencoder/sample-ad-autoencoder.keras')

    info["threshold"] = threshold
    joblib.dump(info, '../../model/sample-ad-autoencoder/sample-ad-autoencoder.info')

    plt.plot(x_train[0])
    plt.plot(x_train_pred[0])
    plt.show()

    print("save model successfully")


if __name__ == '__main__':
    do_train_model()
