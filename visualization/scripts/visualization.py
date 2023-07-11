import streamlit as st
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
from cassandra.cluster import Cluster

def plot_distribution(df, column):
    if column == 'summary':
        fig, ax = plt.subplots(figsize=(10, 6))
        counts, bins, _ = ax.hist(df[column], bins='auto', orientation='horizontal')
        bar_width = 0.8
        ax.barh(bins[:-1], counts, height=bar_width)
        ax.set_xlabel('Frequency')
        ax.set_ylabel(column)
        ax.set_title('Distribution of {}'.format(column))
    else:
        fig, ax = plt.subplots()
        ax.hist(df[column], bins = 'auto')
        ax.set_xlabel(column)
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of {}'.format(column))

    st.pyplot(fig)

def create_area_chart(df, attribute):
    days = df.iloc[:, 0].tolist()
    if attribute == 'kW use':
        min_values = df.iloc[:, 7].tolist()
        max_values = df.iloc[:, 5].tolist()
        avg = df.iloc[:, 3].tolist()
    elif attribute =='kW generation':
        min_values = df.iloc[:, 8].tolist()
        max_values = df.iloc[:, 6].tolist()
        avg = df.iloc[:, 4].tolist()
    elif attribute =='temperature':
        min_values = df.iloc[:, 2].tolist()
        max_values = df.iloc[:, 3].tolist()
        avg = df.iloc[:, 1].tolist()
    elif attribute =='humidity':
        min_values = df.iloc[:, 5].tolist()
        max_values = df.iloc[:, 6].tolist()
        avg = df.iloc[:, 4].tolist()
    elif attribute =='visibility':
        min_values = df.iloc[:, 8].tolist()
        max_values = df.iloc[:, 9].tolist()
        avg = df.iloc[:, 7].tolist()
    elif attribute =='apparentTemperature':
        min_values = df.iloc[:, 11].tolist()
        max_values = df.iloc[:, 12].tolist()
        avg = df.iloc[:, 10].tolist()
    elif attribute =='pressure':
        min_values = df.iloc[:, 14].tolist()
        max_values = df.iloc[:, 15].tolist()
        avg = df.iloc[:, 13].tolist()
    elif attribute =='windSpeed':
        min_values = df.iloc[:, 17].tolist()
        max_values = df.iloc[:, 18].tolist()
        avg = df.iloc[:, 16].tolist()
    elif attribute =='windBearing':
        min_values = df.iloc[:, 20].tolist()
        max_values = df.iloc[:, 21].tolist()
        avg = df.iloc[:, 19].tolist()
    elif attribute =='precipitations':
        min_values = df.iloc[:, 23].tolist()
        max_values = df.iloc[:, 24].tolist()
        avg = df.iloc[:, 22].tolist()
    elif attribute =='dewPoint':
        min_values = df.iloc[:, 26].tolist()
        max_values = df.iloc[:, 27].tolist()
        avg = df.iloc[:, 25].tolist()
    else:
        min_values = [0]
        max_values = [0]
        avg = [0]

    fig, ax = plt.subplots()

    ax.fill_between(days, min_values, max_values, alpha=0.3, label='Range')
    ax.plot(days, avg, color='blue', label='Average' + attribute)

    ax.set_xlabel('Day of the Week')
    ax.set_ylabel('Temperature (°C)')
    ax.set_title('Weekly Temperature Range')

    ax.legend()

    st.pyplot(fig)

def create_bar_chart(df, attribute, granularity):
    fig, ax = plt.subplots()
    period = df.iloc[:, 0].tolist()
    if attribute == 'kW use':
        min_values = df.iloc[:, 7].tolist()
        max_values = df.iloc[:, 5].tolist()
        avg = df.iloc[:, 3].tolist()
        sum = df.iloc[:, 1].tolist()
        ax.set_yscale('log')
    elif attribute =='kW generation':
        min_values = df.iloc[:, 8].tolist()
        max_values = df.iloc[:, 6].tolist()
        avg = df.iloc[:, 4].tolist()
        sum = df.iloc[:, 2].tolist()
        ax.set_yscale('log')
    elif attribute =='temperature':
        min_values = df.iloc[:, 2].tolist()
        max_values = df.iloc[:, 3].tolist()
        avg = df.iloc[:, 1].tolist()
        sum = None
    elif attribute =='humidity':
        min_values = df.iloc[:, 5].tolist()
        max_values = df.iloc[:, 6].tolist()
        avg = df.iloc[:, 4].tolist()
        sum = None
    elif attribute =='visibility':
        min_values = df.iloc[:, 8].tolist()
        max_values = df.iloc[:, 9].tolist()
        avg = df.iloc[:, 7].tolist()
        sum = None
    elif attribute =='apparentTemperature':
        min_values = df.iloc[:, 11].tolist()
        max_values = df.iloc[:, 12].tolist()
        avg = df.iloc[:, 10].tolist()
        sum = None
    elif attribute =='pressure':
        min_values = df.iloc[:, 14].tolist()
        max_values = df.iloc[:, 15].tolist()
        avg = df.iloc[:, 13].tolist()
        sum = None
    elif attribute =='windSpeed':
        min_values = df.iloc[:, 17].tolist()
        max_values = df.iloc[:, 18].tolist()
        avg = df.iloc[:, 16].tolist()
        sum = None
    elif attribute =='windBearing':
        min_values = df.iloc[:, 20].tolist()
        max_values = df.iloc[:, 21].tolist()
        avg = df.iloc[:, 19].tolist()
        sum = None
    elif attribute =='precipitations':
        min_values = df.iloc[:, 23].tolist()
        max_values = df.iloc[:, 24].tolist()
        avg = df.iloc[:, 22].tolist()
        sum = None
    elif attribute =='dewPoint':
        min_values = df.iloc[:, 26].tolist()
        max_values = df.iloc[:, 27].tolist()
        avg = df.iloc[:, 25].tolist()
        sum = None
    else:
        min_values = [0]
        max_values = [0]
        avg = [0]
        sum = None

    bar_width = 0.2
    positions = np.arange(len(period))

    if sum is None:
        ax.bar(positions - bar_width, avg, bar_width, label='Average')
        ax.bar(positions, min_values, bar_width, label='Minimum')
        ax.bar(positions + bar_width, max_values, bar_width, label='Maximum')
    else:
        ax.bar(positions - bar_width, avg, bar_width, label='Average')
        ax.bar(positions, min_values, bar_width, label='Minimum')
        ax.bar(positions + bar_width, max_values, bar_width, label='Maximum')
        ax.bar(positions + 2 * bar_width, sum, bar_width, label='Sum')

    ax.set_xticks(positions)
    ax.set_xticklabels(period)

    ax.set_xlabel(granularity)
    ax.set_ylabel('Value')
    ax.set_title('Statistics')

    ax.legend()

    st.pyplot(fig)

cluster = Cluster(['cassandra'], port='9042')
session = cluster.connect('my_batch')

st.set_page_config(layout="wide")

data_types = ['All', 'kw_use', 'weather']
data_granularities = ['all_time', 'month', 'day_of_week', 'day']

#################
### SELECTION ###
#################

st.sidebar.text('')
st.sidebar.text('')
st.sidebar.text('')

st.sidebar.markdown("**First select the data you want to analyze:** 👇")
data_type = st.sidebar.selectbox(' ', data_types)
if data_type == 'All':
    table_name = 'preprocessed_row_dataset'

else:
    st.sidebar.markdown("**Select the time granularity you want to analyze:** 👇")
    data_granularity = st.sidebar.selectbox(' ', data_granularities)
    table_name = data_type + '_' + data_granularity

query = f"SELECT * FROM {table_name}"
result_set = session.execute(query)

df = pd.DataFrame(list(result_set))

####################
### INTRODUCTION ###
####################

row0_spacer1, row0_1, row0_spacer2, row0_2, row0_spacer3 = st.columns((.1, 2.3, .1, 1.3, .1))
with row0_1:
    st.title('Lambda Architecture - Data Visualization')
with row0_2:
    st.text("")
    st.subheader('Streamlit App by Pietro Baroni and Andrea De Donato')
row3_spacer1, row3_1, row3_spacer2 = st.columns((.1, 3.2, .1))
with row3_1:
    st.markdown("You can find the source code in the GitHub Repository(https://github.com/dedo99/second_project_baronato.git)")

### ALL DATA + DISTRIBUTION###
if data_type == 'All':
    row6_spacer1, row6_1, row6_spacer2 = st.columns((.2, 7.1, .2))
    with row6_1:
        st.subheader("Currently selected data:")

    row3_spacer1, row3_1, row3_spacer2 = st.columns((.2, 7.1, .2))
    with row3_1:
        st.markdown("")
        see_data = st.expander('You can click here to see the raw data first 👉')
        with see_data:
            st.dataframe(data = df.reset_index(drop=True))

    row10_spacer1, row10_1, row10_spacer2 = st.columns((.2, 7.1, .2))
    with row10_1:
        st.subheader('Values Distribution')
    row11_spacer1, row11_1, row11_spacer2, row11_2, row11_spacer3  = st.columns((.2, 2.3, .4, 4.4, .2))
    with row11_1:
        column = st.selectbox ("Of which column you want to see the distribution?", df.columns[1:])
    with row11_2:
        plot_distribution(df, column)

### KW USE DATASET ###
elif data_type == 'kw_use':

    ### AREA PLOT ###
    if data_granularity == 'day':
        row3_spacer1, row3_1, row3_spacer2 = st.columns((.2, 7.1, .2))
        with row3_1:
            st.markdown("")
            see_data = st.expander('You can click here to see the raw data first 👉')
            with see_data:
                st.dataframe(data = df.reset_index(drop=True))
        attributes = ['kW use', 'kW generation']
        row10_spacer1, row10_1, row10_spacer2 = st.columns((.2, 7.1, .2))
        with row10_1:
            st.subheader('Range over time')
        row11_spacer1, row11_1, row11_spacer2, row11_2, row11_spacer3  = st.columns((.2, 2.3, .4, 4.4, .2))
        with row11_1:
            column = st.selectbox ("Of which attribute you want to see the range?", attributes)
        with row11_2:
            create_area_chart(df, column)

    ### BAR PLOT ###
    elif data_granularity == 'month' or data_granularity == 'day_of_week':
        row3_spacer1, row3_1, row3_spacer2 = st.columns((.2, 7.1, .2))
        with row3_1:
            st.markdown("")
            see_data = st.expander('You can click here to see the raw data first 👉')
            with see_data:
                st.dataframe(data = df.reset_index(drop=True))

        attributes_1 = ['kW use', 'kW generation']
        row10_spacer1, row10_1, row10_spacer2 = st.columns((.2, 7.1, .2))
        with row10_1:
            st.subheader('Range over time')
        row11_spacer1, row11_1, row11_spacer2, row11_2, row11_spacer3  = st.columns((.2, 2.3, .4, 4.4, .2))
        with row11_1:
            column = st.selectbox ("Of which attribute you want to see the range?", attributes_1)
        with row11_2:
            create_bar_chart(df, column, data_granularity)

    ### TABLE  ###
    else:
        row6_spacer1, row6_1, row6_spacer2 = st.columns((.2, 7.1, .2))
        with row6_1:
            st.subheader("Currently selected data:")

        row3_spacer1, row3_1, row3_spacer2 = st.columns((.2, 7.1, .2))
        with row3_1:
            st.dataframe(data = df.reset_index(drop=True))

### WEATHER DATASET ###
elif data_type == 'weather':

    ### AREA PLOT ###
    if data_granularity == 'day':
        row3_spacer1, row3_1, row3_spacer2 = st.columns((.2, 7.1, .2))
        with row3_1:
            st.markdown("")
            see_data = st.expander('You can click here to see the raw data first 👉')
            with see_data:
                st.dataframe(data = df.reset_index(drop=True))
        attributes = ['temperature', 'humidity', 'visibility', 'pressure','apparentTemperature', 'windSpeed', 'windBearing', 'precipitations', 'dewPoint']
        row10_spacer1, row10_1, row10_spacer2 = st.columns((.2, 7.1, .2))
        with row10_1:
            st.subheader('Range over time')
        row11_spacer1, row11_1, row11_spacer2, row11_2, row11_spacer3  = st.columns((.2, 2.3, .4, 4.4, .2))
        with row11_1:
            column = st.selectbox ("Of which attribute you want to see the range?", attributes)
        with row11_2:
            create_area_chart(df, column)
    
    ### BAR PLOT ###
    elif data_granularity == 'month' or data_granularity == 'day_of_week':
        row3_spacer1, row3_1, row3_spacer2 = st.columns((.2, 7.1, .2))
        with row3_1:
            st.markdown("")
            see_data = st.expander('You can click here to see the raw data first 👉')
            with see_data:
                st.dataframe(data = df.reset_index(drop=True))
        attributes = ['temperature', 'humidity', 'visibility', 'pressure','apparentTemperature', 'windSpeed', 'windBearing', 'precipitations', 'dewPoint']
        row10_spacer1, row10_1, row10_spacer2 = st.columns((.2, 7.1, .2))
        with row10_1:
            st.subheader('Range over time')
        row11_spacer1, row11_1, row11_spacer2, row11_2, row11_spacer3  = st.columns((.2, 2.3, .4, 4.4, .2))
        with row11_1:
            column = st.selectbox ("Of which attribute you want to see the range?", attributes)
        with row11_2:
            create_bar_chart(df, column, data_granularity)
    
    ### TABLE  ###
    else:
        row6_spacer1, row6_1, row6_spacer2 = st.columns((.2, 7.1, .2))
        with row6_1:
            st.subheader("Currently selected data:")

        row3_spacer1, row3_1, row3_spacer2 = st.columns((.2, 7.1, .2))
        with row3_1:
            st.dataframe(data = df.reset_index(drop=True))



session.shutdown()
cluster.shutdown()