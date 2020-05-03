import numpy as np
import pandas as pd

tau = 2 * np.pi

def fourier(data_frame, data_col, time_col, period, group_col=None):
    wave_vals = data_frame[time_col] * (tau/period)
    wave_vals = pd.concat([
        np.sin(wave_vals).rename('lateral'),
        np.cos(wave_vals).rename('real'),
    ], axis=1).join(data_frame[data_col])
    wave_vals['lateral'] *= wave_vals[data_col]
    wave_vals['real'] *= wave_vals[data_col]
    wave_vals.drop(columns=[data_col], inplace=True)

    if group_col is not None:
        wave_vals = wave_vals.join(data_frame[group_col]).groupby(group_col).mean()
    else:
        wave_vals = wave_vals.mean()

    amplitude = np.sqrt((wave_vals**2).sum(axis=1)) * 2
    phase = np.arctan2(wave_vals['lateral'], wave_vals['real']) * (period/tau)
    phase[phase < 0] += period

    if group_col is not None:
        offset = data_frame.groupby(group_col)[data_col].mean()
        final_data = pd.concat([
            amplitude.rename('amplitude'),
            phase.rename('phase'),
            offset.rename('offset'),
        ], axis=1)
    else:
        offset = data_frame[data_col].mean()
        final_data = pd.Series(
            [amplitude, phase, offset],
            index=['amplitude', 'phase', 'offset'],
        )
        
    return final_data