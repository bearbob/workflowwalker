# WorkflowWalker

Small variant-calling on low read-count samples as is the case in e.g. Panel-seq requires special re-calculations of the gold-standard workflow-results. The re-calculations are a bootstrap procedure which allows to re-evalutate the null-hypothesis e.g. best practice-results by calculating the robustness and re-producability.

Essentially, sub-clonality and chromosome-specific aneuploidy can be calculated by the bootstrap procedure because a maximum-likelihood calculation allows to find the e.g. ploidy which explains the observed results best.

The bootstrap procedure is based om a Simulated Annealing algorithm which governs the choice of aligner, variant caller and respective parameters that are being used to call small variants.
The Simulated Annealing algorithm chooses the next set of algorithms and parameter based on two target functions: 1) Similarity to an a-priori expected number of cluster centers and 2) the similarity to a priorly known gold-standard result set of small variants.

This is a development version that is not intended for publication.