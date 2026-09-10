"""Unit tests for clustering algorithms."""

import unittest

import numpy as np

from taosanalytics.algo.cluster.dbscan import _DBScanService
from taosanalytics.algo.cluster.gmm import _GaussianMixtureService
from taosanalytics.algo.cluster.kmeans import _KMeansService


class TestKMeansService(unittest.TestCase):
    """Tests for KMeans clustering service."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = _KMeansService()
        # Generate sample 2D data with 3 clusters
        np.random.seed(42)
        cluster1 = np.random.randn(20, 2) + np.array([0, 0])
        cluster2 = np.random.randn(20, 2) + np.array([5, 5])
        cluster3 = np.random.randn(20, 2) + np.array([10, 0])
        self.data = np.vstack([cluster1, cluster2, cluster3]).tolist()

    def test_basic_clustering(self):
        """Test basic KMeans clustering."""
        self.service.set_input_data(self.data)
        self.service.set_params({"n_clusters": 3})
        result = self.service.execute()

        self.assertIn("labels", result)
        self.assertIn("centers", result)
        self.assertIn("n_clusters", result)
        self.assertIn("metrics", result)

        self.assertEqual(result["n_clusters"], 3)
        self.assertEqual(len(result["labels"]), 60)
        self.assertEqual(len(result["centers"]), 3)

    def test_parameter_validation(self):
        """Test parameter validation."""
        self.service.set_input_data(self.data)

        # Invalid n_clusters
        with self.assertRaises(ValueError):
            self.service.set_params({"n_clusters": 0})

        with self.assertRaises(ValueError):
            self.service.set_params({"n_clusters": -1})

        # Invalid max_iter
        with self.assertRaises(ValueError):
            self.service.set_params({"max_iter": 0})

        # Invalid init method
        with self.assertRaises(ValueError):
            self.service.set_params({"init": "invalid"})

    def test_empty_data(self):
        """Test with empty data."""
        self.service.set_input_data([])
        with self.assertRaises(ValueError):
            self.service.execute()

    def test_with_timestamps(self):
        """Test clustering with timestamps."""
        ts_list = [f"2023-01-{i:02d}" for i in range(1, 61)]
        self.service.set_input_data(self.data, ts_list)
        self.service.set_params({"n_clusters": 3})
        result = self.service.execute()

        self.assertIn("ts_list", result)
        self.assertEqual(len(result["ts_list"]), 60)

    def test_metrics_calculation(self):
        """Test that metrics are calculated."""
        self.service.set_input_data(self.data)
        self.service.set_params({"n_clusters": 3})
        result = self.service.execute()

        metrics = result["metrics"]
        self.assertIn("silhouette", metrics)
        self.assertIn("davies_bouldin", metrics)
        self.assertIn("calinski_harabasz", metrics)
        self.assertIn("inertia", metrics)

        # Silhouette should be in [-1, 1]
        if metrics["silhouette"] is not None:
            self.assertGreaterEqual(metrics["silhouette"], -1)
            self.assertLessEqual(metrics["silhouette"], 1)


class TestDBScanService(unittest.TestCase):
    """Tests for DBSCAN clustering service."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = _DBScanService()
        # Generate sample 2D data with clear clusters and some noise
        np.random.seed(42)
        cluster1 = np.random.randn(20, 2) + np.array([0, 0])
        cluster2 = np.random.randn(20, 2) + np.array([10, 10])
        noise = np.random.uniform(-5, 15, (5, 2))
        self.data = np.vstack([cluster1, cluster2, noise]).tolist()

    def test_basic_clustering(self):
        """Test basic DBSCAN clustering."""
        self.service.set_input_data(self.data)
        self.service.set_params({"eps": 1.5, "min_samples": 5})
        result = self.service.execute()

        self.assertIn("labels", result)
        self.assertIn("centers", result)
        self.assertIn("n_clusters", result)
        self.assertIn("n_noise", result)
        self.assertIn("metrics", result)

        self.assertEqual(len(result["labels"]), 45)
        self.assertGreaterEqual(result["n_clusters"], 0)

    def test_parameter_validation(self):
        """Test parameter validation."""
        self.service.set_input_data(self.data)

        # Invalid eps
        with self.assertRaises(ValueError):
            self.service.set_params({"eps": 0})

        with self.assertRaises(ValueError):
            self.service.set_params({"eps": -1})

        # Invalid min_samples
        with self.assertRaises(ValueError):
            self.service.set_params({"min_samples": 0})

    def test_noise_detection(self):
        """Test that DBSCAN detects noise points."""
        # Create data with clear separation and isolated points
        np.random.seed(42)
        cluster1 = np.random.randn(30, 2) + np.array([0, 0])
        isolated = np.array([[20, 20], [25, 25], [-20, -20]])
        self.data = np.vstack([cluster1, isolated]).tolist()

        self.service.set_input_data(self.data)
        self.service.set_params({"eps": 1.0, "min_samples": 5})
        result = self.service.execute()

        # Should detect some noise points
        self.assertGreater(result["n_noise"], 0)


class TestGaussianMixtureService(unittest.TestCase):
    """Tests for Gaussian Mixture Model clustering service."""

    def setUp(self):
        """Set up test fixtures."""
        self.service = _GaussianMixtureService()
        # Generate sample 2D data with 3 Gaussian components
        np.random.seed(42)
        cluster1 = np.random.randn(20, 2) + np.array([0, 0])
        cluster2 = np.random.randn(20, 2) + np.array([5, 5])
        cluster3 = np.random.randn(20, 2) + np.array([10, 0])
        self.data = np.vstack([cluster1, cluster2, cluster3]).tolist()

    def test_basic_clustering(self):
        """Test basic GMM clustering."""
        self.service.set_input_data(self.data)
        self.service.set_params({"n_components": 3})
        result = self.service.execute()

        self.assertIn("labels", result)
        self.assertIn("centers", result)
        self.assertIn("n_clusters", result)
        self.assertIn("probabilities", result)
        self.assertIn("metrics", result)

        self.assertEqual(result["n_clusters"], 3)
        self.assertEqual(len(result["labels"]), 60)
        self.assertEqual(len(result["centers"]), 3)
        self.assertEqual(len(result["probabilities"]), 60)

    def test_parameter_validation(self):
        """Test parameter validation."""
        self.service.set_input_data(self.data)

        # Invalid n_components
        with self.assertRaises(ValueError):
            self.service.set_params({"n_components": 0})

        # Invalid covariance_type
        with self.assertRaises(ValueError):
            self.service.set_params({"covariance_type": "invalid"})

        # Invalid max_iter
        with self.assertRaises(ValueError):
            self.service.set_params({"max_iter": 0})

    def test_soft_clustering(self):
        """Test that GMM produces soft cluster assignments."""
        self.service.set_input_data(self.data)
        self.service.set_params({"n_components": 3})
        result = self.service.execute()

        probabilities = result["probabilities"]

        # Each sample should have probabilities summing to ~1.0
        for prob_row in probabilities:
            self.assertAlmostEqual(sum(prob_row), 1.0, places=5)

        # Each probability should be in [0, 1]
        for prob_row in probabilities:
            for prob in prob_row:
                self.assertGreaterEqual(prob, 0)
                self.assertLessEqual(prob, 1)

    def test_metrics_bic_aic(self):
        """Test that BIC and AIC are calculated."""
        self.service.set_input_data(self.data)
        self.service.set_params({"n_components": 3})
        result = self.service.execute()

        metrics = result["metrics"]
        self.assertIn("bic", metrics)
        self.assertIn("aic", metrics)

        # BIC and AIC should be real numbers
        self.assertIsInstance(metrics["bic"], (int, float))
        self.assertIsInstance(metrics["aic"], (int, float))


class TestClusteringIntegration(unittest.TestCase):
    """Integration tests for clustering services."""

    def test_1d_data_conversion(self):
        """Test that 1D data must be 2D for clustering algorithms."""
        # 1D data should be converted in the handler layer, not here
        # Clustering services expect 2D data (n_samples, n_features)
        data_1d = [1, 2, 3, 4, 5, 10, 11, 12, 13, 14]
        service = _KMeansService()
        service.set_input_data(data_1d)
        service.set_params({"n_clusters": 2})

        # Should raise ValueError for 1D input
        with self.assertRaises(ValueError):
            service.execute()

    def test_all_algorithms_same_data(self):
        """Test that all algorithms can process the same data."""
        np.random.seed(42)
        cluster1 = np.random.randn(15, 2) + np.array([0, 0])
        cluster2 = np.random.randn(15, 2) + np.array([5, 5])
        data = np.vstack([cluster1, cluster2]).tolist()

        algorithms = [
            (_KMeansService(), {"n_clusters": 2}),
            (_DBScanService(), {"eps": 1.5, "min_samples": 3}),
            (_GaussianMixtureService(), {"n_components": 2}),
        ]

        for service, params in algorithms:
            with self.subTest(algo=service.name):
                service.set_input_data(data)
                service.set_params(params)
                result = service.execute()

                self.assertEqual(len(result["labels"]), 30)
                self.assertIn("metrics", result)
                self.assertGreater(result["n_clusters"], 0)


if __name__ == "__main__":
    unittest.main()
