import numpy as np

class Cluster:
	def __init__(self, data):
		self.size = len(data)
		# self.data = data
		cluster_data = data[:, 2:]
		self.sums = sum(cluster_data)

		dimensions = len(self.sums)

		sum_sq = [0 for x in range(dimensions)]
		for i in range(self.size):
			for j in range(dimensions):
				sum_sq[j] += cluster_data[i][j] ** 2
		self.sum_sq = np.array(sum_sq)
		self.std_devs = self.stdDev()

	def addPoint(self, point):
		self.size += 1
		# self.data = np.append(self.data, point)
		data_point = point[2:]

		self.sums += data_point
		self.sum_sq += data_point ** 2
		dimensions = len(self.sums)

		for j in range(dimensions):
			self.sum_sq[j] += data_point[j] ** 2
		self.std_devs = self.stdDev()
		return self

	def stdDev(self):
		variance = self.sum_sq / self.size - (self.sums / self.size) ** 2
		return np.sqrt(variance)

	def mahalanobis_distance(self, point):
		mean_cluster = self.sums / self.size
		difference = point - mean_cluster
		
		idx = np.where(self.std_devs != 0)
		
		normalized = difference[idx] / self.std_devs[idx]
		total = np.dot(normalized, normalized)
		return np.sqrt(total)