import numpy as np

tau = 2 * np.pi

def wave(time_col, period, amplitude, phase, offset=0):
    return (np.cos((time_col - phase) * (tau/period)) * amplitude) + offset