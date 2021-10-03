# DentHlthHx

This repo has the bones of several recent projects. I have included no underlying data or our UM utility libraries
but you should have a feel for how I work from these examples. 

## D3 Student Feedback

This is a classic NLP sentiment-and-topic analysis of the student feedback surveys we receive from most of the D3s on most days. 

Academic Affairs is interested in the mood of the class. They want to know where the bulk of their concerns rest at a given time
so they can offer better mentoring and support in the clinics. It's an `R`project running over `textminR` and the tidyverse. The
audience is at the associate Dean level. I expect to refine and publish this, please do not share outside your team. I've included the RMarkdown code as well as the `html` output.

When working on NLP in `R`, I use the methodology in "Tidy Text Mining in R" because it interleaves
so well with other `R`-based tidy data tools. When doing NLP work in `python`, I generally transform into matrices that are tractable 
for computation with `sklearn` and its friends.

## Rotation Analysis

The school's leadership came to us with a perception that when students change their rotations, it creates a cascade of chaos. 
This analysis uses graph techniques to assess that claim and found that, in fact, students generally manage the rotation
swaps well on their own and that the bulk of the workload associated with the changes was making clerical changes to the
EHR. We did uncover patterns of cliques within the school that led to leadership investigating whether students who are not
part of a strong sub-community might find it harder to change rotations - they are coming at these results more from a 
Diversity, Equity, and Inclusion direction than form one of working out clerical workload.  This work was done in a mix of `R` and `python`,
and I found myself switching tools for the sake of performance or for simple transfers between Neo4j and the iGraph toolset.

I've included the `python` and `R` code as well as the `html` output from the RMarkdown. I generally prefer `python` for database pipeline work and some analytical efforts, but I prefer `R` when preparing
manuscript-quality data visualizations. When the final product is going to a research team, I use `R` exclusively, as it's
the native analytical tool in the biosciences.

## Health History Parsing

Important context - this project was in full flight when COVID hit. We lost almost all our front-line staff,
and my job was instantly transformed from half-production, half-research to all-production plus basic help center support. I had to down tools on a messy project and some of the approaches I was using at the time have been supplanted by pretrained word-vector approaches. The school is only now moving back into somewhat more normal behaviour and I am actively working on the next version.

The files here include:

* some notes to myself about how to use this material for constructing the whole thing
* a talk I delivered just before COVID about how this all comes together
* `map_hx_text.py` loads and maps our EHR data into a parsed, graph-data-friendly form
* `load_umls_graph.py` builds the Neo4j dataset and binds it to the UMLS graph data
* `connect_patients.py` builds a homogeneous graph of patient nodes that is suitable for graph learning tools

This project is the one I am preparing to reanimate and bring into my doctoral research.
