import numpy as np
from sklearn.cluster import KMeans
import math
from cluster import Cluster
import sys

def output_line(ds_clusters, cs_clusters, rs_clusters, num):
	ds_total = 0
	for clusterid, cluster in ds_clusters.items():
		ds_total += cluster.size
	
	cs_total = 0
	for clusterid, cluster in cs_clusters.items():
		cs_total += cluster.size
	return 'Round %d: %d,%d,%d,%d' % (num, ds_total, len(cs_clusters), cs_total, len(rs_clusters))

def bfr_main(data_to_add, ds_clusters, cs_clusters, rs_clusters, ds_point_cluster_results, threshold):
	for point in data_to_add:
		min_dis = 999999
		min_clusterid = -100

		data_point = point[2:]
		for clusterid in ds_clusters.keys():
			cluster = ds_clusters[clusterid]
			distance = cluster.mahalanobis_distance(data_point)

			if distance < threshold:
				if distance < min_dis:
					min_dis = distance
					min_clusterid = clusterid

		if min_clusterid != -100:
			cluster = ds_clusters[min_clusterid]
			cluster = cluster.addPoint(point)
			ds_clusters[min_clusterid] = cluster
			
			points = ds_point_cluster_results[min_clusterid]
			points.append(point)
			ds_point_cluster_results[min_clusterid] = points
		else:
			# No ds set found, add into CS set
			for clusterid in cs_clusters.keys():
				cluster = cs_clusters[clusterid]
				distance = cluster.mahalanobis_distance(data_point)

				if distance < threshold:
					if distance < min_dis:
						min_dis = distance
						min_clusterid = clusterid
			if min_clusterid != -100:
				cluster = cs_clusters[min_clusterid]
				cluster = cluster.addPoint(point)
				cs_clusters[min_clusterid] = cluster
			else:
				rs_clusters.append(point)
	return ds_clusters, cs_clusters, rs_clusters, ds_point_cluster_results

def main():
	if len(sys.argv) < 4:
		print('Not enough arguments provided')
		return

	input_file = sys.argv[1]
	n_clusters = int(sys.argv[2])
	output_file_name = sys.argv[3]
	writer = open(output_file_name, 'w')
	writer.write('The intermediate results:\n')

	data = np.loadtxt(input_file, delimiter=',')
	percent = 0.2

	total = len(data)
	sampled_data = data[: int(total * percent), :]
	cluster_data = sampled_data[:, 2:]

	K = int(10 * n_clusters)
	kmeans = KMeans(n_clusters=K, random_state=0).fit(cluster_data)


	kmeans_result = kmeans.predict(cluster_data)
	cluster_cnt_dict = {}
	for kmeansid in kmeans_result:
		cnt = cluster_cnt_dict.get(kmeansid, 0)
		cnt = cnt + 1
		cluster_cnt_dict[kmeansid] = cnt

	one_clusters = set()
	for cluster_id, cnt in cluster_cnt_dict.items():
		if cnt == 1:
			one_clusters.add(cluster_id)

	rs = []
	rs_index = set()
	for i in range(len(kmeans_result)):
		if kmeans_result[i] in one_clusters:
			rs.append(sampled_data[i])
			rs_index.add(i)

	rs = np.asarray(rs)
	reamining_sampled_data = []
	for i in range(len(sampled_data)):
		if not i in rs_index:
			reamining_sampled_data.append(sampled_data[i])
	reamining_sampled_data = np.asarray(reamining_sampled_data)

	K = n_clusters
	remaining_cluster_data = reamining_sampled_data[:, 2:]

	ds_kmeans = KMeans(n_clusters=K, random_state=0).fit(remaining_cluster_data)
	kmeans_result = ds_kmeans.predict(remaining_cluster_data)

	# Initialized DS set
	ds = (reamining_sampled_data, kmeans_result)
	ds_point_cluster_results = {}

	ds_clusters = {}
	for i in range(len(kmeans_result)):
		point = reamining_sampled_data[i]
		clusterid = kmeans_result[i]
		
		points = ds_clusters.get(clusterid, [])
		points.append(point)
		ds_clusters[clusterid] = points
		ds_point_cluster_results[clusterid] = points
		
	for clusterid in ds_clusters.keys():
		ds_clusters[clusterid] = Cluster(np.array(ds_clusters[clusterid]))

	# Find compression set
	rs_data = rs[:, 2:]
	K = int(0.8 * len(rs))
	rs_kmeans = KMeans(n_clusters=K, random_state=0).fit(rs_data)
	rs_kmeans_result = rs_kmeans.predict(rs_data)

	# Separate CS and RS set
	cs_rs_clusters = {}
	for i in range(len(rs_kmeans_result)):
		point = rs[i]
		clusterid = rs_kmeans_result[i]
		
		points = cs_rs_clusters.get(clusterid, [])
		points.append(point)
		cs_rs_clusters[clusterid] = points

	# Initialize CS and RS set
	cs_clusters = {}
	rs_clusters = []
	for clusterid, points in cs_rs_clusters.items():
		if len(points) > 1:
			cs_clusters[clusterid] = Cluster(np.array(points))
		else:
			rs_clusters.append(points[0])
	line_str = output_line(ds_clusters, cs_clusters, rs_clusters, 1)
	print(line_str)
	writer.write(line_str + '\n')

	threshold = 2 * math.sqrt(len(data[0][2:]))

	start = int(total * percent)
	end = start + int(total * percent)
	num_round = 2

	while start < total:
		
		ds_clusters, cs_clusters, rs_clusters, ds_point_cluster_results = bfr_main(data[start:end], ds_clusters, cs_clusters, rs_clusters, ds_point_cluster_results, threshold)
		line_str = output_line(ds_clusters, cs_clusters, rs_clusters, num_round)
		print(line_str)
		writer.write(line_str + '\n')
		start = end
		end = start + int(total * percent)
		num_round = num_round + 1

	writer.write('\nThe clustering results:\n')
	final_cluster_map = {}
	for clusterid, points in ds_point_cluster_results.items():
		for point in points:
			final_cluster_map[int(point[0])] = int(clusterid)

	for point in data:
		if point[0] not in final_cluster_map:
			final_cluster_map[int(point[0])] = -1

	results = []
	for id, clusterid in final_cluster_map.items():
		results.append((id, clusterid))
	results.sort(key=lambda x: x[0], reverse=False)

	for result in results:
		writer.write('%d,%d\n' % (result[0], result[1]))
	writer.close()

main()