from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import requests
import datetime
import time
import json
from sklearn.cluster import KMeans
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
 
import credentials as cd # Credentials are store in a GIT IGNORE'ed file


#################### TODOs############################
'''


'''

################################################################################

##Global
# Influx authentication
client = DataFrameClient(cd.host, cd.port, cd.usr, cd.passwd, cd.db, ssl=True, verify_ssl=True)

def main():

	

	# Get current time and time x=52 weeks ago for querrzing influxdb
	now_str, from_time_str = getCurrentTime(weeks=52)

	# Get comfort data from learnign trail for the last 15min

	group_user_df = queryLearningTrail(to_time_str = now_str, from_time_str = from_time_str)
	#TODO: Add some if statements so that we don't run this if there is no data uploaded in the last x min
	
	#Get sensing data for the following id
	# TODO: map the learning trail location input to the equivalent sensor
	#sensor_df = queryInflux(sensor_id = "ASP017001822", now_str = now_str, from_time_str = from_time_str)


	#Initialise pdf
	pp = PdfPages('../figures/learningTrailResults.pdf')

	plotLearningTrail(group_user_df, pp, selectColumns = ['preferCooler', 'preferWarmer', 'thermalComfy', 'preferQuieter', 'preferLouder', 'noiseComfy', 'preferDimmer', 'preferBrighter', 'lightComfy'])
	plotLearningTrail(group_user_df, pp, selectColumns = ['preferCooler', 'preferWarmer', 'thermalComfy'])
	plotLearningTrail(group_user_df, pp, selectColumns = ['preferQuieter', 'preferLouder', 'noiseComfy'])
	plotLearningTrail(group_user_df, pp, selectColumns = ['preferDimmer', 'preferBrighter', 'lightComfy'])
	pp.close()


def getCurrentTime(weeks):
	#current time, not sure if I should be using Sing time or now
	now = datetime.datetime.utcnow()
	#the time 1 year ago
	time_ago = now - datetime.timedelta(weeks=weeks)

	#convert time to a format for querrying influxdb
	now_str = now.strftime('%Y-%m-%dT%H:%M:%SZ')
	from_time_str = time_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

	#returning as global variable - TODO tidy this up to avoid future issues
	return(now_str, from_time_str)


def queryLearningTrail(to_time_str, from_time_str):
	print("querying learnign trail database on influx db ")

	# Query Influx
	result = client.query("SELECT thermal, noise, light FROM people.autogen.learningTrail WHERE time > '{}' AND time < '{}' GROUP BY room, user_id_web".format(from_time_str,to_time_str))

	# Create emtpy dataframe
	learningTrail_df = pd.DataFrame()

	# Iterate through groups (rooms, and users)
	for key in result:
		# Get data frame belonging to a group
		current_df = result[key]
		# Set the location and user id to the data
		current_df["location"] =  key[1][0][1]
		current_df["user_id"] = key[1][1][1]
		# Concat this sub dataframe to the main result data frame
		learningTrail_df = pd.concat([learningTrail_df, current_df], sort=False)

	# Remove old duplicate data where -1,0,1 was used
	learningTrail_df = learningTrail_df[(learningTrail_df != 0).all(1)]


	print(learningTrail_df)
	learningTrail_df.to_csv("../data/learningTrail.csv")

	print("averaged data is")

	learningTrail_df['preferCooler'] = learningTrail_df['thermal'][learningTrail_df.thermal == 11.0] #Too Hot
	learningTrail_df['preferWarmer'] = learningTrail_df['thermal'][learningTrail_df.thermal == 9.0] # Too COld
	learningTrail_df['thermalComfy'] = learningTrail_df['thermal'][learningTrail_df.thermal == 10.0] # COmfy
	
	learningTrail_df['preferQuieter'] = learningTrail_df['noise'][learningTrail_df.noise == 11.0] # Too Loud
	learningTrail_df['preferLouder'] = learningTrail_df['noise'][learningTrail_df.noise == 9.0] # Too Quiet
	learningTrail_df['noiseComfy'] = learningTrail_df['noise'][learningTrail_df.noise == 10.0] # Comfy

	learningTrail_df['preferDimmer'] = learningTrail_df['light'][learningTrail_df.light == 11.0] # Too bright
	learningTrail_df['preferBrighter'] = learningTrail_df['light'][learningTrail_df.light == 9.0] # Too Dark
	learningTrail_df['lightComfy'] = learningTrail_df['light'][learningTrail_df.light == 10.0] # Comfy

	print(learningTrail_df)
	group_user_df=learningTrail_df.groupby('user_id').count()

	group_user_df = group_user_df[group_user_df.thermal >=5]

	group_user_df[['preferCooler', 'preferWarmer', 'thermalComfy']] = group_user_df[['preferCooler', 'preferWarmer', 'thermalComfy']].div(group_user_df.thermal, axis=0)
	group_user_df[['preferQuieter', 'preferLouder', 'noiseComfy']] = group_user_df[['preferQuieter', 'preferLouder', 'noiseComfy']].div(group_user_df.noise, axis=0)
	group_user_df[['preferDimmer', 'preferBrighter', 'lightComfy']] = group_user_df[['preferDimmer', 'preferBrighter', 'lightComfy']].div(group_user_df.light, axis=0) 

	print(group_user_df)
	group_user_df.drop(["thermal", "light", "noise", "location"], axis=1, inplace=True)

	return(group_user_df)
	# average_lt_df = learningTrail_df.groupby('location').mean()
	# average_lt_df["count"] = learningTrail_df.groupby('location').count()["thermal"]
	# print(average_lt_df)
	# return learningTrail_df

def plotLearningTrail(group_user_df, pp, selectColumns = ['preferCooler', 'preferWarmer', 'thermalComfy']):



	clusterplot = sns.clustermap(group_user_df[selectColumns], cmap="Blues", metric="euclidean", method="single")
	#Remove Y ticks 
	ax = clusterplot.ax_heatmap
	ax.axes.get_yaxis().set_visible(False)

	plt.savefig(pp, format="pdf")


def queryInflux(sensor_id, now_str, from_time_str):
	print("querrying InfluxDB senSING Sensors")

	#Querry Database
	result = client.query("SELECT temperature, noise, light FROM spaces.autogen.senSING WHERE id = '{}' AND time > '{}' AND time < '{}'".format(sensor_id, from_time_str,now_str))
	
	sensor_df = result['senSING']
	sensor_df = sensor_df.resample("1min").mean() 

	return sensor_df




#Just in case we end up importing funcitons from this file
if __name__ == "__main__":

	main()
