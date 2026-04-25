/* ===== NIVEDITHA SRIKANTH — PERSONALIZED AI CHATBOT v4 ===== */
(function () {
  'use strict';

  const EMAIL = 'sniveditha006@gmail.com';
  const GH    = 'https://github.com/Niveditha-04';

  function contactLine() {
    return `<br><br>Want to talk more? <a href="mailto:${EMAIL}">Email me ↗</a> or <a href="${GH}?tab=repositories" target="_blank">browse all my code on GitHub ↗</a>`;
  }
  function projectFooter(repoUrl) {
    return `<br><br><a href="${repoUrl}" target="_blank">📂 View Code on GitHub ↗</a> &nbsp;·&nbsp; <a href="mailto:${EMAIL}">✉️ Email me for details</a>`;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // KNOWLEDGE BASE — Niveditha's real answers, in her own voice
  // ─────────────────────────────────────────────────────────────────────────
  const KB = {

    greeting: [
      "Hey! I'm Niveditha — data scientist, researcher, and someone who genuinely loves building intelligent systems. Ask me anything.",
      "Hi! I'm Niveditha Srikanth. Ask me about my work, projects, research, or anything else — I'll give you a straight answer.",
    ],

    // ── WHO ARE YOU ──────────────────────────────────────────────────────
    identity: {
      patterns: [/who are you\b|tell me about you(rself)?\b|introduce yourself|^about you$|who is niveditha|describe yourself|unique.*about you|things about you|more about you/i],
      answer: `I'm a data scientist with a biotech background who stumbled into ML and never looked back. I build things end-to-end — from messy raw data to a working pipeline — and I bring a research mindset to engineering problems. That means I don't just run a model, I actually want to know <em>why</em> it's doing what it's doing.<br><br>
Right now I'm steering hard toward AI/ML engineering — building and deploying intelligent systems at scale, not just notebooks. My background is unusual: I've read gene expression papers, worked in a biofilm lab, published 12+ papers at IEEE and Springer, and built fraud detection pipelines on 10M+ claim records at Cigna. That cross-domain range is probably the most underrated thing about how I work.<br><br>
<strong>The thread:</strong> healthcare ML, bioinformatics, cloud engineering, NLP, Bayesian modelling — all of it connects because I want to work on problems where the data is complex, the stakes are real, and the answer actually matters to someone.<br><br>
<em>Biotechnology by origin. Data science by choice. AI engineering by direction.</em>`,
      followups: ["What's your current role?", "What projects have you built?", "What research have you done?"]
    },

    // ── CURRENT ROLE ──────────────────────────────────────────────────────
    current_role: {
      patterns: [/current(ly)?|right now|where do you work|cigna|your (job|role|position)|what do you do/i],
      answer: `I'm a <strong>Data Science Associate Analyst at The Cigna Group – Cigna Healthcare</strong> (Fortune 500).<br><br>
The thing I'm most proud of there is building the composite risk scoring framework. I engineered 40+ features from 10M+ claim records, designed a log-scaled and percentile-normalised scoring layer, and made the output interpretable enough for fraud investigators to actually act on it — surfaced through a Power BI dashboard. The pipeline contributed to an estimated <strong>$400K–$700K in potential savings</strong> and improved detection coverage by 20%.<br><br>
The model accuracy was one part of it. Building something a human can pick up and immediately know what to investigate — that was the harder and more important part.${contactLine()}`,
      followups: ["Tell me about your fraud detection project", "What's your tech stack at work?", "What other roles have you had?"]
    },

    // ── EXPERIENCE ───────────────────────────────────────────────────────
    experience: {
      patterns: [/experience|work(ed)?|intern(ship)?|past (role|job)|career|employment|your background/i],
      answer: `Here's my full career so far:<br>
• <strong>Cigna Healthcare</strong> — Data Science Associate Analyst <em>(current)</em><br>
• <strong>AI Quantalytics / VihaanAI CyberLabs</strong> — Research & Data Science Associate<br>
• <strong>Crayon Data Pvt Ltd</strong> — Machine Learning & NLP Intern<br>
• <strong>Bioinformatics Lab, REC</strong> — Undergraduate Research Assistant (Computational ML)<br>
• <strong>All Mind AI / Zion Robotics</strong> — Jr. Research Assistant<br>
• <strong>Centre of Excellence in Biofilms, REC</strong> — Microbial Biofilms Intern<br><br>
Each role added a different layer — industry engineering, applied research, robotics AI, biotech. That cross-domain experience is what I think makes me different from someone who's only ever been in one lane.${contactLine()}`,
      followups: ["Tell me about your Cigna work", "What did you do at Crayon Data?", "Tell me about your education"]
    },

    // ── CRAYON DATA ──────────────────────────────────────────────────────
    crayon: {
      patterns: [/crayon|crayon data|nlp intern|ml intern|internship.*nlp|nlp.*internship/i],
      answer: `At <strong>Crayon Data</strong>, I worked on NLP pipelines processing <strong>100K+ customer records</strong> using TF-IDF and Word2Vec for text representation. I also supported a <strong>BERT-based text classification pipeline</strong> that hit ~85% accuracy on internal benchmarks — my first real exposure to transformer-based models in a production-adjacent setting.<br><br>
I also built dimensionality reduction workflows (PCA, t-SNE) and sentiment/topic trend visualisations. It was where the full NLP workflow clicked for me — beyond academic tutorials into something that actually ran on real customer data.${contactLine()}`,
      followups: ["Tell me about your NLP projects", "What's your tech stack?", "Other internship experience?"]
    },

    // ── EDUCATION ────────────────────────────────────────────────────────
    education: {
      patterns: [/educat|degree|study|studied|college|university|iit|rajalakshmi|school|academic|qualif/i],
      answer: `Two degrees:<br>
• <strong>B.Tech in Biotechnology</strong> — Rajalakshmi Engineering College, Tamil Nadu<br>
• <strong>Micro-Degree in Computer Science</strong> — IIT Guwahati (credit-linked program)<br><br>
Biology taught me something a CS degree alone wouldn't have — how to think about messy, high-dimensional, noisy data where ground truth is genuinely hard to establish. When I worked on gene expression data with 49,000 features and no clean label, I understood <em>biologically</em> why the noise was there. That context makes you a better ML practitioner.${contactLine()}`,
      followups: ["Why did you go from biotech to data science?", "Tell me about your research", "What's your background?"]
    },

    // ── WHY BIOTECH → DATA SCIENCE ───────────────────────────────────────
    why_switch: {
      patterns: [/why.*biotech|why.*biology|leave.*bio|bio.*to.*data|switch.*data|how.*get into.*ml|why.*data science/i],
      answer: `I didn't leave biology — I just found a more powerful lens to look at it through.<br><br>
Somewhere in the middle of my biotech degree, I realised the most interesting questions in biology — why does this gene cluster behave this way, what's driving this disease pathway — were being answered by computation, not pipelines. I got into ML not to escape biology but because I wanted to solve biological problems at a scale a lab bench couldn't give me.<br><br>
My gene clustering work on T2D-atherosclerosis, my CVA prognosis model, my bioinformatics research — that's still all biology. I just speak it in Python and R now.`,
      followups: ["Tell me about your gene clustering project", "Tell me about your bioinformatics work", "What's your current focus?"]
    },

    // ── PROJECTS OVERVIEW ────────────────────────────────────────────────
    projects_overview: {
      patterns: [/what projects|your projects|projects you('ve| have)|show.*projects|list.*projects|projects.*built|built.*projects|all your projects|projects.*work/i],
      answer: `I've built 15+ projects across healthcare ML, bioinformatics, cloud engineering, NLP, and marketing analytics:<br><br>
<strong>Healthcare & Biology:</strong><br>
• CVA Prognosis — 99.93% accuracy (Random Forest)<br>
• Healthcare Fraud Detection — 10M+ claims, 20% improved coverage<br>
• Maternal Risk Prediction — 85–90% accuracy<br>
• Heart Disease Prediction (Logistic Regression)<br>
• Gene Expression Clustering — 3 hub genes identified (T2D/atherosclerosis)<br><br>
<strong>Cloud & Data Engineering:</strong><br>
• Log Analytics Pipeline on AWS<br>
• ETL Pipeline for E-Commerce Payments (AWS)<br>
• Smart City Management Platform<br><br>
<strong>NLP & Vision:</strong><br>
• Scientific Paper Clustering (TF-IDF + embeddings)<br>
• Fake News Detection — PA Classifier, 92–96% accuracy<br>
• Face Recognition (LBPH / OpenCV)<br>
• Intelligent Image Enhancer<br><br>
<strong>Analytics:</strong><br>
• Bayesian MMM — 10–15% budget reallocation recommendation<br>
• Ad Performance Analytics (CTR, CPC, ROI)<br>
• Smart Online Banking System<br><br>
<a href="${GH}?tab=repositories" target="_blank">📂 See all projects on GitHub ↗</a>`,
      followups: ["Tell me about your healthcare ML work", "Which project are you most proud of?", "Tell me about your cloud projects"]
    },

    // ── PROJECT: CVA PROGNOSIS ───────────────────────────────────────────
    project_stroke: {
      patterns: [/stroke|cerebrovascular|cva|prognosis/i],
      answer: `<strong>Cerebrovascular Accident (CVA) Prognosis using Supervised ML</strong><br><br>
<strong>What it is:</strong> An ML system predicting stroke outcomes from patient clinical features.<br>
<strong>What it does:</strong> Compares multiple classifiers and selects the best for stroke prognosis with probability scores.<br>
<strong>Results:</strong><br>
• Random Forest — <strong>99.93% accuracy</strong> (winner)<br>
• XGBoost — 97.94%<br>
• Decision Tree — 97.53%<br><br>
<strong>Dataset:</strong> ~43,400 patient records (publicly available clinical stroke data). Applied label encoding, mean imputation for missing values, and hyperparameter tuning via grid search.<br><br>
<strong>Published at:</strong> IEEE WCONF 2023. Also registered as a <strong>software copyright</strong>.${projectFooter(GH + '/Cerebrovascular-Accident-Prognosis-using-Supervised-ML')}`,
      followups: ["What accuracy did you achieve?", "Tell me about your other healthcare projects", "Your publications?"]
    },

    // ── PROJECT: GENE CLUSTERING ─────────────────────────────────────────
    project_gene: {
      patterns: [/gene|mclust|immune.*check|checkpoint|t2d|diabetes.*athero|gene.*express|bioinformatic.*cluster/i],
      answer: `<strong>Mclust-Based Gene Expression Clustering for Immune Checkpoint Analysis</strong><br><br>
<strong>What it is:</strong> Independent bioinformatics research connecting T2D and atherosclerosis at the molecular level.<br>
<strong>What it does:</strong> Started with <strong>49,000 genes</strong> (GEO dataset GSE57329), reduced to ~1,000 through sequential statistical filtering (98% reduction), then ran PCA + Mclust. The 9-cluster VVV model achieved a <strong>silhouette score of 0.81</strong>.<br><br>
<strong>Key finding:</strong> Three hub genes stood out — <strong>CD4, CXCL10, and FMO3</strong> — validated through PPI network analysis (STRING + Cytoscape) and pathway enrichment via KEGG and Reactome. These are immune checkpoint regulators dysregulated at the intersection of diabetes and cardiovascular disease — with real downstream relevance for therapeutic targeting.<br><br>
<strong>Tools:</strong> R (Mclust, ggplot2, Rtsne), STRING, Cytoscape, KEGG, Reactome.${projectFooter(GH + '/Immune-Gene-Clustering-for-T2D-Atherosclerosis')}`,
      followups: ["What were the hub genes you found?", "What R packages did you use?", "Your bioinformatics publications?"]
    },

    // ── PROJECT: FRAUD DETECTION ─────────────────────────────────────────
    project_fraud: {
      patterns: [/fraud|anomaly.*detect|claims.*fraud|billing.*anomaly|insurance.*anomaly|healthcare.*fraud/i],
      answer: `<strong>Healthcare Claims Fraud & Anomaly Detection Pipeline</strong><br><br>
<strong>What it is:</strong> End-to-end fraud detection pipeline on healthcare insurance claims data.<br>
<strong>What it does:</strong> Combines rule-based SQL flagging, <strong>BIRCH clustering</strong> for provider segmentation, <strong>LOF (Local Outlier Factor)</strong> for outlier scoring, and IQR/Z-score flagging as an anomaly ranking layer. At Cigna, I also used log-scaled transformations and CUME_DIST-based percentile normalisation to build a composite risk score.<br><br>
<strong>Scale & impact:</strong><br>
• 1M+ claims processed<br>
• ~3% flagged by fraud rules; top 1% (~128K claims) priority-queued<br>
• Detection coverage improved by <strong>20%</strong>, anomaly lift improved ~11%<br>
• Contributed to estimated <strong>$400K–$700K in potential savings</strong><br>
• Output surfaced via Power BI dashboard used by investigators daily${projectFooter(GH + '/Healthcare-Claims-Fraud-Anomaly-Detection')}`,
      followups: ["What anomaly methods did you use?", "Tell me about your Cigna work", "Other data engineering projects?"]
    },

    // ── PROJECT: LOG ANALYTICS ───────────────────────────────────────────
    project_logs: {
      patterns: [/log.*analytic|log.*pipeline|aws.*log|log.*aws|log.*engineer/i],
      answer: `<strong>Log Analytics Pipeline on AWS</strong><br><br>
<strong>What it is:</strong> A cloud-native log data engineering pipeline built entirely on AWS.<br>
<strong>What it does:</strong> Real-time log ingestion, transformation, and analytics-ready storage — full data lifecycle from raw stream to queryable insights.<br>
<strong>Used for:</strong> Operational monitoring and anomaly detection at scale — the kind of infrastructure that underpins production systems.${projectFooter(GH + '/Log-Analytics-Pipeline-on-AWS')}`,
      followups: ["What AWS services did you use?", "Tell me about your ETL project", "Your cloud engineering experience?"]
    },

    // ── PROJECT: PAPER CLUSTERING ─────────────────────────────────────────
    project_paper: {
      patterns: [/paper.*cluster|cluster.*paper|scientific.*similar|similarity.*paper|nlp.*paper|research.*cluster/i],
      answer: `<strong>Scientific Paper Clustering for Similarity Discovery</strong><br><br>
<strong>What it is:</strong> An NLP pipeline grouping scientific papers by semantic similarity.<br>
<strong>What it does:</strong> TF-IDF vectorisation and clustering to find thematic groups across large corpora — so researchers can discover related work buried under different terminology.<br>
<strong>Used for:</strong> Literature discovery and research mapping, especially useful in interdisciplinary fields.${projectFooter(GH + '/Scientific-Paper-Clustering-for-similarity')}`,
      followups: ["What clustering algorithm did you use?", "Tell me about your fake news project", "What NLP tools do you use?"]
    },

    // ── PROJECT: BAYESIAN MMM ─────────────────────────────────────────────
    project_mmm: {
      patterns: [/bayesian|marketing.*mix|mmm|spend.*optim|budget.*optim|marketing.*model/i],
      answer: `<strong>Bayesian Marketing Mix Modelling (MMM)</strong><br><br>
<strong>What it is:</strong> Full probabilistic model for marketing budget optimisation across channels.<br>
<strong>What it does:</strong> Models 5 channels from YouTube ads data (paid video, display, search-adjacent, cross-channel), engineered features including <strong>CPC, ROAS, CTR, and impression-weighted ROI per channel</strong>. Uses rstanarm for Bayesian inference — capturing diminishing returns, adstock, and saturation curves — with tidyverse for preprocessing.<br><br>
<strong>Recommendation:</strong> Shift 10–15% of budget from low-ROI channels to higher-performing ones, with uncertainty quantification showing how confident the recommendation is — not just what it says.<br><br>
The insight from the data: CTR and ROI didn't move together. High-CTR channels had terrible downstream conversion. Stop optimising for clicks; optimise for ROAS-weighted conversions.${projectFooter(GH + '/Bayesian-Marketing-Spend-Optimization-MMM')}`,
      followups: ["What channels did you model?", "Tell me about your ad analytics project", "What Python libraries did you use?"]
    },

    // ── PROJECT: SMART CITY ───────────────────────────────────────────────
    project_smart_city: {
      patterns: [/smart.?city|city.*management|urban.*data|city.*platform/i],
      answer: `<strong>Smart City Management Platform</strong><br><br>
<strong>What it is:</strong> A data platform integrating multiple city-scale data streams for urban management.<br>
<strong>What it does:</strong> Data ingestion, processing, and analytics to support smart decision-making across city operations — turning disparate municipal data into coordinated, actionable insights.${projectFooter(GH + '/Smart-City-Management-Platform')}`,
      followups: ["Tell me about your ETL project", "What's your cloud engineering background?", "Other platform projects?"]
    },

    // ── PROJECT: ETL E-COMMERCE ───────────────────────────────────────────
    project_etl: {
      patterns: [/etl|e.?commerce.*pay|payment.*ecommerce|aws.*etl|ecommerce.*aws/i],
      answer: `<strong>ETL Pipeline for E-Commerce Payments on AWS</strong><br><br>
<strong>What it is:</strong> Production-ready ETL pipeline for e-commerce payment data on AWS.<br>
<strong>What it does:</strong> Extracts payment transaction data, applies transformations and data quality checks, loads into analytics-ready storage — covering schema handling, error recovery, and scalable processing.<br>
<strong>Used for:</strong> Clean, reliable payment data pipelines that analytics and finance teams can trust.${projectFooter(GH + '/ETL-AWS-E-commerce-Payments')}`,
      followups: ["What AWS services did you use?", "Tell me about your log analytics project", "Your data engineering background?"]
    },

    // ── PROJECT: BANKING ─────────────────────────────────────────────────
    project_banking: {
      patterns: [/banking|bank(ing)?.*system|online.*bank|smart.*bank/i],
      answer: `<strong>Smart Online Banking System</strong><br><br>
<strong>What it is:</strong> A data-driven smart banking application with ML-powered transaction monitoring.<br>
<strong>What it does:</strong> Core banking workflows with anomaly identification on transactions — intelligent monitoring built into a practical financial system.${projectFooter(GH + '/Smart-Online-Banking-System')}`,
      followups: ["Tell me about your fraud detection project", "What ML methods did you use?", "Your financial analytics work?"]
    },

    // ── PROJECT: FACE RECOGNITION ─────────────────────────────────────────
    project_face: {
      patterns: [/face.?recogni|face.?detect|lbph|computer vision|opencv|face.*algo/i],
      answer: `<strong>Face Recognition & Detection using LBPH Algorithm</strong><br><br>
<strong>What it is:</strong> Real-time face recognition using classical computer vision — no GPU required.<br>
<strong>What it does:</strong> Implements the Local Binary Pattern Histogram (LBPH) algorithm with OpenCV. LBPH is inherently strong on small, controlled datasets — reliable in controlled conditions, which is its known sweet spot before you need something heavier like a CNN-based approach.<br>
<strong>Used for:</strong> Identity verification and access control — demonstrates how classical CV methods remain highly effective for constrained real-world scenarios.${projectFooter(GH + '/Face-Regonition-and-Detection-using-LBPH-Algorithm')}`,
      followups: ["What accuracy did it achieve?", "Tell me about your image enhancer project", "Other CV projects?"]
    },

    // ── PROJECT: JOB SCHEDULER ────────────────────────────────────────────
    project_scheduler: {
      patterns: [/job.?schedul|iit.*schedul|schedul.*algo|task.*queue|iit.*project/i],
      answer: `<strong>IIT-G Job Scheduler</strong><br><br>
<strong>What it is:</strong> Job scheduling system built during my IIT Guwahati Computer Science program.<br>
<strong>What it does:</strong> Implements and compares scheduling algorithms — priority queues, round-robin, SJF — for efficient resource allocation and task queue management.<br>
<strong>Why it matters:</strong> This systems-level thinking directly informs how I approach pipeline design and data orchestration in production.${projectFooter(GH + '/IIT-G2406_T1_Job_Scheduler')}`,
      followups: ["Tell me about your IIT education", "Other algorithms work?", "What's your systems background?"]
    },

    // ── PROJECT: HEART DISEASE ────────────────────────────────────────────
    project_heart: {
      patterns: [/heart.?disease|logistic.*heart|heart.*predict|cardiac.*predict/i],
      answer: `<strong>Heart Disease Prediction using Logistic Regression</strong><br><br>
<strong>What it is:</strong> Clinical ML model predicting heart disease risk from patient health indicators.<br>
<strong>What it does:</strong> Logistic regression with feature engineering and model evaluation — outputs probability of heart disease per patient profile.<br>
<strong>Note:</strong> This was one of my earliest healthcare ML projects and shaped my direction toward medical data science.${projectFooter(GH + '/Logistic-Regression-to-predict-Heart-Disease')}`,
      followups: ["Tell me about your CVA project", "Your maternal risk project?", "Other healthcare ML work?"]
    },

    // ── PROJECT: FAKE NEWS ────────────────────────────────────────────────
    project_fakenews: {
      patterns: [/fake.?news|misinformation|vectorizer.*classifier|pa.*classifier|passive.*aggressive|news.*detect/i],
      answer: `<strong>Fake News Detection using Vectorizer & Passive-Aggressive Classifier</strong><br><br>
<strong>What it is:</strong> NLP text classification pipeline detecting fake news articles.<br>
<strong>What it does:</strong> TF-IDF Vectorisation converts news text into features; a Passive-Aggressive Classifier — an online learning algorithm built for text — labels articles as real or fake. Accuracy lands in the <strong>92–96% range</strong> (PA classifiers are particularly well-suited here because they handle the class boundary aggressively and update incrementally).<br>
<strong>Used for:</strong> Real-world misinformation detection — practical NLP on one of the most relevant social problems in the information age.${projectFooter(GH + '/Fake-news-detection-and-prediction-using-Vectorizer-and-PA-classifier')}`,
      followups: ["What accuracy did you achieve?", "Tell me about your paper clustering project", "Other NLP projects?"]
    },

    // ── PROJECT: AD ANALYTICS ─────────────────────────────────────────────
    project_ads: {
      patterns: [/ad.?performance|ctr|cpc|roi.*ad|youtube.*ad|ad.*analytic|marketing.*analytic/i],
      answer: `<strong>Ad Performance Analytics — CTR, CPC & ROI with YouTube Ads</strong><br><br>
<strong>What it is:</strong> Marketing analytics project optimising YouTube ad campaign performance.<br>
<strong>What it does:</strong> Analyses CTR, CPC, and ROI across campaigns; segments audiences; surfaces which creative/targeting combinations deliver the best efficiency.<br><br>
<strong>The most surprising insight:</strong> CTR and ROI didn't move together the way you'd expect. Some channels had high click-through rates but terrible downstream ROI — people clicked but didn't convert. The Bayesian model exposed that disconnect clearly. The real takeaway: stop optimising for clicks, optimise for <strong>ROAS-weighted conversions</strong>.${projectFooter(GH + '/Analyzing-Ad-Performance-CTR-CPC-and-ROI-Optimization-with-Youtube-ads-Data')}`,
      followups: ["Tell me about your Bayesian MMM project", "What analytics tools do you use?", "Other marketing analytics work?"]
    },

    // ── PROJECT: MATERNAL RISK ────────────────────────────────────────────
    project_maternal: {
      patterns: [/maternal|pregnancy|prenatal|women.*risk|risk.*pregnan|maternity/i],
      answer: `<strong>Maternal Risk Prediction during Pregnancy</strong><br><br>
<strong>What it is:</strong> ML model predicting health risk levels (low / mid / high) for women during pregnancy.<br>
<strong>Features used:</strong> Age, blood sugar levels, blood pressure (systolic/diastolic), body temperature, heart rate — standard clinical indicators.<br>
<strong>Performance:</strong> <strong>85–90% accuracy</strong> across classifier variants (Random Forest + logistic regression baseline). Dataset: UCI/Kaggle maternal health records.<br>
<strong>Why it matters:</strong> Early flagging of high-risk pregnancies enables timely clinical intervention. A cause I genuinely care about.${projectFooter(GH + '/Maternal-Risk-Prediction-for-women-during-pregnancy')}`,
      followups: ["What accuracy did you achieve?", "Tell me about your heart disease project", "Other healthcare ML projects?"]
    },

    // ── PROJECT: IMAGE ENHANCER ───────────────────────────────────────────
    project_image: {
      patterns: [/image.?enhanc|pixel.?sharp|image.?sharp|intelligent.*image|image.*process.*enhanc/i],
      answer: `<strong>Intelligent Image Enhancer & Pixel Sharpener</strong><br><br>
<strong>What it is:</strong> Intelligent image processing tool for adaptive quality enhancement.<br>
<strong>What it does:</strong> Adaptive pixel sharpening, contrast enhancement, and noise reduction — intelligently selecting enhancement level based on input image characteristics.<br>
<strong>Used for:</strong> Image quality improvement for downstream computer vision tasks or direct user-facing applications.${projectFooter(GH + '/Intelligent-Image-Enhancer-and-Pixel-Sharpener')}`,
      followups: ["Tell me about your face recognition project", "What image processing libraries did you use?", "Other CV projects?"]
    },

    // ── SKILLS ───────────────────────────────────────────────────────────
    skills: {
      patterns: [/skill|tech.*stack|what.*tools|languages you|what do you know|proficient|speciali[sz]|your.*stack/i],
      answer: `My full tech stack:<br><br>
<strong>Languages:</strong> Python, R, SQL<br>
<strong>ML/AI:</strong> scikit-learn, PyTorch, TensorFlow, XGBoost, PyMC, rstanarm, OpenAI GPT<br>
<strong>Bioinformatics:</strong> Mclust, Bioconductor, ggplot2, Rtsne, igraph, Cytoscape<br>
<strong>NLP:</strong> TF-IDF, Word2Vec, BERT, spaCy, NLTK, Transformers (HuggingFace)<br>
<strong>CV:</strong> OpenCV, PIL<br>
<strong>Cloud & Engineering:</strong> AWS (SageMaker, Athena, S3, Lambda), Docker, CI/CD, Git<br>
<strong>Visualisation:</strong> Power BI, Tableau, Matplotlib, Seaborn, Plotly<br><br>
The unusual part: R and bioinformatics tooling sit right alongside production Python and AWS. Most data scientists have one or the other — I use both, and in the same day.${contactLine()}`,
      followups: ["Tell me about your AWS projects", "What ML methods do you use most?", "Your bioinformatics tools?"]
    },

    // ── PUBLICATIONS ─────────────────────────────────────────────────────
    publications: {
      patterns: [/pub(lication)?|ieee|springer|journal|research.*paper|academic.*paper|paper.*publish|how many paper/i],
      answer: `I have <strong>12+ publications</strong> at IEEE, Springer, and Elsevier covering:<br>
• ML for healthcare (stroke prognosis, fraud detection, malware classification)<br>
• Brain tumour segmentation with deep learning (GlobalNet + FusionNet)<br>
• NLP (speech segmentation, resume screening, grid computing)<br>
• Bioinformatics and gene analysis<br>
• Fintech and VLSI design<br><br>
<strong>2 Best Paper / Presentation awards.</strong> I also hold 2 software copyrights and co-authored a UK design patent.<br><br>
Check the Publications section on this page for the full list with direct links.${contactLine()}`,
      followups: ["What are your best paper awards for?", "Tell me about your research areas", "What's your most important paper?"]
    },

    // ── ACHIEVEMENTS ─────────────────────────────────────────────────────
    achievements: {
      patterns: [/achiev|award|prize|hackathon|databricks|walmart|sparkathon|sparkwars|competi|won|honor/i],
      answer: `Key wins:<br>
• <strong>1st Prize</strong> — DISS FEST 2024, Oral Presentation<br>
• <strong>Top 10</strong> — Databricks Sparkwars Hackathon<br>
• <strong>Participant</strong> — Walmart Sparkathon<br>
• <strong>Volunteer & Learning Circle Leader</strong> — U&I organisation<br>
• <strong>2 Best Paper / Presentation awards</strong> at IEEE & Springer conferences<br>
• <strong>2 Software Copyrights</strong> (Indian Copyright Office)<br>
• Co-author on a <strong>UK Design Patent</strong>${contactLine()}`,
      followups: ["Tell me about your publications", "Your research experience?", "What hackathons did you join?"]
    },

    // ── MOST PROUD PROJECT ────────────────────────────────────────────────
    proud_project: {
      patterns: [/most proud|proudest|favourite.*project|best.*project|project.*love|tell me.*proud/i],
      answer: `The gene clustering work — not because it was the most technically complex, but because it was entirely mine. I designed the pipeline, chose the approach, validated the results, and interpreted the biology.<br><br>
Starting with <strong>49,000 genes</strong>, reducing to 1,000 through statistical filtering, running PCA + Mclust, landing on three hub genes — CD4, CXCL10, FMO3 — with real immunological relevance, validated through published network analysis.<br><br>
That project sits at the exact intersection of where I want to live: rigorous ML applied to biological questions that genuinely matter. No supervisor gave me that direction — I just followed what I wanted to understand.`,
      followups: ["Tell me more about the gene clustering project", "What were the key findings?", "What projects did you do at Cigna?"]
    },

    // ── RESEARCH ─────────────────────────────────────────────────────────
    research: {
      patterns: [/research|bioinformatic|lab.*work|research.*background|academic.*work|science.*work/i],
      answer: `Research has always run in parallel with my engineering work:<br>
• <strong>Computational Bioinformatics</strong> at REC's lab (Dr. Sujata Roy) — ML applied to genomics<br>
• <strong>AI/ML research</strong> at AI Quantalytics<br>
• <strong>Robotics AI</strong> at Zion Robotics / All Mind AI<br>
• <strong>Independent research</strong> — gene clustering in T2D/atherosclerosis<br><br>
The independent work is what I'm most proud of — no supervisor, no team, just a biology question I wanted to answer with data. That's when I knew research was something I'd always keep doing alongside engineering.${contactLine()}`,
      followups: ["Tell me about your gene clustering project", "What are your publications?", "Your biotech background?"]
    },

    // ── BIOTECH BACKGROUND ───────────────────────────────────────────────
    biotech: {
      patterns: [/biotech|biology|life science|biofilm|microbial|wet lab/i],
      answer: `My undergrad was Biotechnology at Rajalakshmi Engineering College. I worked in a biofilm lab, studied gene expression papers, and interned at REC's Centre of Excellence in Biofilms under Dr. Saravanan Periasamy.<br><br>
What that gave me — beyond domain knowledge — is an understanding of messy, high-dimensional, noisy data where the ground truth is genuinely hard to establish. When I worked on 49,000 genes with no clean label, I understood <em>biologically</em> why the noise was there. That context makes you a fundamentally better ML practitioner than someone who only ever worked with clean benchmark datasets.${contactLine()}`,
      followups: ["How did you get into data science?", "Tell me about your bioinformatics research", "Your gene clustering project?"]
    },

    // ── CONTACT ───────────────────────────────────────────────────────────
    contact: {
      patterns: [/contact|email|reach|hire|connect|social|get in touch|how.*contact|message you/i],
      answer: `Best way to reach me: <a href="mailto:${EMAIL}"><strong>${EMAIL}</strong></a><br><br>
I'm also on:<br>
• <a href="${GH}/" target="_blank">GitHub — github.com/Niveditha-04 ↗</a><br>
• <a href="https://www.researchgate.net/scientific-contributions/S-Niveditha-2240278935" target="_blank">ResearchGate — research profile ↗</a><br><br>
Always open to the right conversation — data science, ML engineering, AI research, or anything at the biology-data intersection.`,
      followups: ["What roles are you open to?", "Download your resume?", "Tell me about your experience"]
    },

    // ── RESUME ────────────────────────────────────────────────────────────
    resume: {
      patterns: [/resum[eé]|cv|download.*resume|my resume/i],
      answer: `My resume is on this page — hit the <strong>"Download My Resume"</strong> button in the hero section at the top. It covers my full experience, skills, publications, and projects.${contactLine()}`,
      followups: ["What's your experience?", "Contact you directly?", "What skills do you have?"]
    },

    // ── OPEN TO WORK ─────────────────────────────────────────────────────
    availability: {
      patterns: [/open to|looking for|availab|recruit|hire you|roles.*interest|job.*search|next role/i],
      answer: `I want to be in <strong>AI/ML Engineering</strong> — not just a data scientist who hands models off, but someone who builds, deploys, and maintains intelligent systems end-to-end.<br><br>
Ideally at a company where ML is core to the product — healthtech, fintech, or an AI-native startup. The problem space I'm most drawn to: large-scale data pipelines and model deployment — fraud detection, clinical decision support, recommendation systems.<br><br>
Title-wise: <strong>ML Engineer, AI Engineer, or Applied Scientist</strong>. Somewhere I can grow from building models to architecting the systems that run them.<br><br>
Reach me at <a href="mailto:${EMAIL}">${EMAIL}</a> — let's talk.`,
      followups: ["What's your current role?", "Download your resume?", "What's your tech stack?"]
    },

    // ── IDEAL ROLE / 5 YEARS ─────────────────────────────────────────────
    future: {
      patterns: [/5 year|five year|where.*see yourself|long.*term|future.*goal|career.*goal/i],
      answer: `In 5 years I want to be in an AI engineering or applied ML role where I'm architecting intelligent systems — not just training models, but designing the full pipeline from data ingestion to model deployment to monitoring.<br><br>
Ideally in healthcare or biomedical AI, because that's where my domain knowledge compounds. I also want to stay close to research — publishing, reviewing, contributing — because the best engineers in AI are the ones who haven't lost the researcher's instinct to question their own assumptions.`,
      followups: ["What roles are you open to now?", "Tell me about your AI engineering experience", "Contact you?"]
    },

    // ── WHY SHOULD WE HIRE YOU ────────────────────────────────────────────
    why_hire: {
      patterns: [/why.*hire|hire.*you|what.*stand out|what.*unique|advantage.*you|you.*over.*other/i],
      answer: `Because the problems worth solving aren't pure CS problems.<br><br>
In healthcare AI, bioinformatics, and fraud detection — the data has context that a pure CS background doesn't come with. I bring ML engineering skills <em>and</em> the domain intuition to know when a result is biologically implausible, clinically irrelevant, or statistically suspicious in a way that matters.<br><br>
I've also sat with a fraud investigator trying to explain why a model flagged a claim. I've had to justify a clustering result to someone who knows what CD4 expression means physiologically. That cross-domain fluency is rare, and in healthcare AI — which is where some of the most important underdeveloped ML work is happening — it's a real edge.`,
      followups: ["Tell me about your healthcare experience", "What's your biggest project?", "Contact you?"]
    },

    // ── BIGGEST WEAKNESS ─────────────────────────────────────────────────
    weakness: {
      patterns: [/weakness|weak.*point|improv.*area|could.*better|what.*you.*struggle|what.*challenge.*you/i],
      answer: `I go deep. When I'm working on a problem I care about, I can over-engineer the solution — add one more feature, run one more validation — when sometimes the right call is to ship and iterate.<br><br>
I've gotten better at setting internal checkpoints and asking: <em>"Is this additional complexity actually improving the output for the end user?"</em> That question has saved me from a lot of unnecessary rabbit holes.`,
      followups: ["Tell me about how you approach problems", "How do you handle ambiguity?", "What motivates you?"]
    },

    // ── BEHAVIORAL — AMBIGUOUS DATA ───────────────────────────────────────
    behavior_ambiguous: {
      patterns: [/ambiguous|unclear.*problem|messy.*data|no.*label|no.*ground.*truth|unstructured/i],
      answer: `At Cigna, I was handed 10M+ claim records with the goal of surfacing fraudulent billing — but fraud wasn't cleanly labeled. No ground truth handed to me.<br><br>
I had to design the detection framework from scratch: define what anomalous looked like, engineer features that made those patterns visible, and build a scoring layer investigators could trust. The answer was to start with statistical baselines and layer from there — constantly validating with the fraud team rather than disappearing into the model for weeks. That feedback loop was the actual methodology.`,
      followups: ["Tell me more about the fraud detection project", "What methods did you use?", "What was the impact?"]
    },

    // ── BEHAVIORAL — LEARNING FAST ────────────────────────────────────────
    behavior_learning: {
      patterns: [/learn.*fast|learn.*quickly|new.*skill.*pressure|pick up|ramp.*up/i],
      answer: `When I started at Cigna, I had to get productive on AWS SageMaker, Athena, and large-scale SQL almost immediately — at a data volume and production context I hadn't worked at before.<br><br>
I didn't pretend I knew it. I learned fast, asked targeted questions, and leaned on documentation. Within the first few weeks I was writing queries across 10M+ records and building pipeline components. The pressure helped — I work well when there's a real deadline attached to the learning.`,
      followups: ["Tell me about your AWS experience", "What tools do you use at Cigna?", "What's your tech stack?"]
    },

    // ── BEHAVIORAL — FAILURE / SETBACKS ──────────────────────────────────
    behavior_failure: {
      patterns: [/fail|setback|mistake|went.*wrong|didn't work|bad.*outcome|how.*handle.*failure/i],
      answer: `I treat it like a bad chess game — you review it, you understand what the actual mistake was (not just the symptom), and you move on.<br><br>
The CVA project had several false starts before the model architecture clicked. The fraud detection framework went through multiple scoring designs before investigators found it actionable. I don't find failure demotivating — I find it clarifying. The failure iterations were just the actual work.`,
      followups: ["Tell me about your CVA project", "What's your approach to problem-solving?", "What are you most proud of?"]
    },

    // ── BEHAVIORAL — COMMUNICATING TECHNICAL RESULTS ─────────────────────
    behavior_communication: {
      patterns: [/communicat|non.*technical|explain.*to|stakeholder|translate.*result|present.*result|dashboard/i],
      answer: `At Cigna, my anomaly detection outputs fed into a Power BI dashboard that fraud investigators used daily — people who are not data scientists.<br><br>
I had to design the output so a claim's risk score was immediately interpretable: what's flagged, why it's flagged, and what to look at first. That meant translating percentile-normalised composite scores into plain priority tiers.<br><br>
The lesson: if you have to explain your model's output every time someone uses it, the output isn't designed well enough.`,
      followups: ["Tell me more about the fraud detection pipeline", "Your Cigna experience?", "What tools did you use for the dashboard?"]
    },

    // ── PERSONALITY / WHAT DRIVES YOU ────────────────────────────────────
    personality: {
      patterns: [/passion|what drives|what.*motivat|curious|what.*different|what.*unique|what.*makes you/i],
      answer: `What makes me different is the range — and the fact that I actually connect it. I can write a bioinformatics analysis in R, build an AWS data pipeline, publish an academic paper, and run a Bayesian model — and connect them into one coherent thought.<br><br>
I'm most engaged when I'm working on a problem I haven't figured out yet. That's why I move between industry work and independent research simultaneously. I'd rather be a T-shaped person with a very long vertical.`,
      followups: ["What projects excite you most?", "Where do you see yourself in 5 years?", "What kind of role are you looking for?"]
    },

    // ── HOBBIES / PERSONAL ────────────────────────────────────────────────
    personal: {
      patterns: [/hobby|hobbies|outside work|personal|chess|paint|watercolor|travel|movie/i],
      answer: `I play chess — I genuinely enjoy the pattern recognition and the idea that every game is a new optimisation problem. In ML work that shows up as: before I commit to a modelling approach, I map out what could go wrong — data drift, label imbalance, deployment constraints.<br><br>
I also do watercolour painting, which is almost the opposite of how I think analytically — it rewards letting go of control a little. I travel when I can (new places force you to observe things you'd otherwise automate), and I watch a lot of movies. I like stories that don't resolve neatly.`,
      followups: ["How does chess influence your work?", "Tell me about yourself", "What drives you professionally?"]
    },

    // ── BLOG ─────────────────────────────────────────────────────────────
    blog: {
      patterns: [/blog|write|article|t2dm|galactic|diamond.*nerd|molecular.*mind/i],
      answer: `I write about things I find genuinely fascinating — T2DM research, genetic discoveries, galactic origins, molecular neuroscience, and more. Check the <strong>Blog</strong> section of this site. I write to understand, not just to share — it's part of how I process complex science.${contactLine()}`,
      followups: ["Your research background?", "What are your publications?", "Tell me about your bioinformatics work"]
    },

    // ── LOCATION ─────────────────────────────────────────────────────────
    location: {
      patterns: [/locat|city|country|where.*based|where are you|india|us|usa|tamil/i],
      answer: `Based in the US, originally from Tamil Nadu, India. My academic roots are at Rajalakshmi Engineering College (Chennai area) and IIT Guwahati — two very different environments that shaped how I think about breadth and depth simultaneously.${contactLine()}`,
      followups: ["Tell me about your education", "What's your current role?", "How to contact you?"]
    },

    // ── CONTACT / GITHUB ALL ──────────────────────────────────────────────
    github_all: {
      patterns: [/github|all.*repos?|more.*projects?|full.*list|other.*projects?/i],
      answer: `My full GitHub has 15+ repositories across healthcare ML, bioinformatics, cloud engineering, NLP, CV, and analytics — all public and documented.<br><br>
<a href="${GH}?tab=repositories" target="_blank">📂 Browse all repositories ↗</a><br><br>
Pick any project and ask me about it — I can walk you through the approach, methods, and what it was built for.`,
      followups: ["Tell me about your healthcare projects", "Your cloud/AWS work?", "NLP or vision projects?"]
    },

    // ── THANKS ───────────────────────────────────────────────────────────
    thanks: {
      patterns: [/thank|thanks|great|awesome|cool|nice|perfect|helpful|love it/i],
      answer: `Glad that was useful! Keep asking — I'm happy to go deeper on any project, method, or research area.`,
      followups: ["Tell me about your projects", "Your research?", "How to contact you?"]
    },

    // ── CURRENTLY DOING / UPSKILLING ─────────────────────────────────────────
    currently_doing: {
      patterns: [/up to (now|these days)|what.*doing (right )?now|what.*currently doing|what.*working on (now|currently|right now)|doing these days|what are you up to/i],
      answer: `Right now I'm fully focused on upskilling in <strong>AI and ML engineering</strong> — building real, end-to-end projects that go beyond notebooks into actual deployable systems.<br><br>
I'm working on projects covering the full ML pipeline: data ingestion, feature engineering, model training, deployment, and monitoring. The focus is on closing the gap between knowing ML and engineering it — building things that actually run, scale, and do something in the world.<br><br>
It's a very deliberate investment in my next level. I'm levelling up fast and working on some very interesting projects in this space. If you'd like to talk about what I'm building or about opportunities, <a href="mailto:sniveditha006@gmail.com">email me directly ↗</a> — always open to the right conversation.`,
      followups: ["What roles are you open to?", "What's your tech stack?", "How can I contact you?"]
    },

    // ── WHAT IS CRAYONDATA ────────────────────────────────────────────────────
    company_crayon: {
      patterns: [/what.*is.*crayon|what.*crayon.*do|crayon.*company|kind.*company.*crayon|crayon.*platform|crayon.*product/i],
      answer: `<strong>CrayonData</strong> is a Singapore-based AI company that builds personalisation platforms for large enterprises — banks, fintechs, and travel companies. Their core product is <strong>maya.ai</strong>, an AI engine that analyses customer data to generate tailored product recommendations and improve customer experience.<br><br>
They've reached 127M+ customers globally and work with enterprise clients who need to turn massive datasets into individual-level personalisation at scale.<br><br>
I interned there as an ML & NLP Intern, working on NLP pipelines over 100K+ customer records — TF-IDF, Word2Vec, and a BERT-based text classifier. A great place to see what production-scale personalisation ML actually looks like.`,
      followups: ["What NLP work did you do at Crayon?", "Tell me about your internship experience", "What's your current role?"]
    },

    // ── WHAT IS AI QUANTALYTICS ───────────────────────────────────────────────
    company_quantalytics: {
      patterns: [/what.*quantalytics|what.*vihaanai|ai quantalytics.*company|quantalytics.*do|vihaanai.*company/i],
      answer: `<strong>AI Quantalytics</strong> is an AI research and analytics firm focused on applying machine learning to financial data — predicting performance of stocks, ETFs, and other instruments. It was significant enough to be acquired by Forbes Media.<br><br>
<strong>VihaanAI CyberLabs</strong> is a research lab focused on AI and computer vision technologies for real-world applications — the kind of org where you do applied AI research, not just deploy existing models.<br><br>
I worked across both as a Research & Data Science Associate — doing applied ML research, which is where I built a lot of my research habits before moving into industry at Cigna.`,
      followups: ["Tell me about your research experience", "What's your current role?", "Your publications from that time?"]
    },

    // ── WHAT IS CIGNA ─────────────────────────────────────────────────────────
    company_cigna: {
      patterns: [/what.*is.*cigna|what.*cigna.*do|cigna.*company|kind.*company.*cigna|cigna.*business|about.*cigna/i],
      answer: `<strong>The Cigna Group</strong> is a Fortune 500 multinational health insurance and managed care company — ranked #33 on the Fortune 500 with ~$195B in annual revenue. They operate globally, serving 180M+ customer relationships across 30+ countries.<br><br>
Their main businesses: <strong>Cigna Healthcare</strong> (medical, dental, disability, life insurance benefits) and <strong>Evernorth Health Services</strong> (pharmacy, care, and benefits management). One of the largest health services organisations in the world.<br><br>
That scale is why the fraud detection work there was so interesting — when you're processing 10M+ claim records, the engineering and statistical problems are genuinely hard in ways a smaller dataset wouldn't expose.`,
      followups: ["What did you work on at Cigna?", "Tell me about your fraud detection project", "What's your current status?"]
    },

    // ── WHAT IS RAJALAKSHMI ENGINEERING COLLEGE ───────────────────────────────
    university_rec: {
      patterns: [/rajalakshmi|rec.*college|rec.*uni|your.*college|your.*university|where.*study|thandalam|sriperumbudur/i],
      answer: `<strong>Rajalakshmi Engineering College (REC)</strong> is one of the top private engineering colleges in Tamil Nadu — ranked in the Top 5 in the state and #101–150 nationally in the NIRF 2025 Engineering category. It's located in Thandalam, Sriperumbudur near Chennai.<br><br>
REC is known for strong research output, industry connections, and dedicated labs — including the Centre of Excellence in Biofilms and a Bioinformatics Research Lab, both of which I worked in during my undergrad.<br><br>
It's where I published my first research, built my first ML models on clinical data, and first understood that computation and biology were pointing at the same problems from different directions.`,
      followups: ["What did you study there?", "Tell me about your research at REC", "What degree do you have?"]
    },

    // ── WHAT IS THE DAKSH GURUKUL / IIT GUWAHATI COURSE ─────────────────────
    course_iitg: {
      patterns: [/daksh.?gurukul|iit.*course|iit.*program|micro.?degree|iitg.*skill|iit.*guwahati.*course/i],
      answer: `The <strong>Daksh Gurukul</strong> is India's first credit-linked skill programme by <strong>IIT Guwahati</strong>, in partnership with NSDC and recognised by the Ministry of Skill Development. It bridges the gap between academic degrees and industry-ready engineering — taught by IIT professors, not MOOCs.<br><br>
I enrolled in the <strong>Computer Science Engineering with AI</strong> track to directly complement my biotechnology degree with formal CS foundations. The programme includes data structures, algorithms, systems thinking, and AI — things a biotech curriculum naturally skips.<br><br>
Why I took it: I wanted the rigour of an IIT credential in CS alongside my applied ML experience. It filled a specific gap and gave me the systems-level thinking that now shows up in how I design data pipelines and approach ML architecture.`,
      followups: ["What did you study at IIT Guwahati?", "Tell me about your education", "How did you get into data science?"]
    },

    // ── WHAT IS ATHEROSCLEROSIS ───────────────────────────────────────────────
    domain_atherosclerosis: {
      patterns: [/atherosclerosis|arterial.*plaque|plaque.*artery|hardening.*artery|artery.*narrow/i],
      answer: `<strong>Atherosclerosis</strong> is the hardening and narrowing of arteries due to plaque buildup — fats, cholesterol, calcium, and other substances accumulating on arterial walls over time. It often begins forming in childhood and worsens silently for decades.<br><br>
The consequences are severe: restricted blood flow can cause <strong>heart attack, stroke, vascular dementia, and heart failure</strong>. Atherosclerosis-linked diseases are the leading cause of death globally. Risk factors include high LDL cholesterol, hypertension, Type 2 diabetes, smoking, and genetic predisposition.<br><br>
This is directly relevant to my research — my gene clustering work found that <strong>CD4, CXCL10, and FMO3</strong> are dysregulated at the intersection of T2D and atherosclerosis. These immune checkpoint genes point to an inflammatory pathway linking diabetes to cardiovascular disease — which has real implications for therapeutic targeting.`,
      followups: ["Tell me about your gene clustering research", "What's the link between T2D and atherosclerosis?", "What hub genes did you find?"]
    },

    // ── WHAT IS TYPE 2 DIABETES ───────────────────────────────────────────────
    domain_t2d: {
      patterns: [/type.?2 diabet|t2d|t2dm|type ii diabet|insulin resist|diabetes.*mellitus/i],
      answer: `<strong>Type 2 Diabetes (T2D)</strong> is a chronic metabolic disorder — accounting for ~90% of all diabetes cases — where the body's cells become resistant to insulin. The pancreas initially compensates by producing more insulin, but gradually loses capacity, causing blood sugar to rise dangerously.<br><br>
The systemic effects are far-reaching: T2D patients face <strong>2–4x increased cardiovascular risk</strong>, plus microvascular damage causing retinopathy (vision loss), nephropathy (kidney failure), and neuropathy (nerve damage). It's not just a blood sugar problem — it reshapes the entire inflammatory and metabolic landscape.<br><br>
My bioinformatics research sits right at this intersection: I studied the gene expression overlap between T2D and atherosclerosis using GEO dataset GSE57329. The hub genes I identified — <strong>CD4, CXCL10, FMO3</strong> — are immune regulators, suggesting that the T2D → cardiovascular disease pathway runs through immune dysregulation, not just metabolic dysfunction. That's the kind of finding that ML on genomic data can surface that a single lab experiment cannot.`,
      followups: ["Tell me about your gene clustering research", "What is atherosclerosis?", "What hub genes did you find?"]
    },

    // ── MOST IMPORTANT PUBLICATION ───────────────────────────────────────────
    pub_important: {
      patterns: [/most important.*p(aper|ub)|best paper|main publication|significant.*paper|key.*paper|notable.*paper|which.*paper.*proud/i],
      answer: `Two answers depending on what you mean by "important".<br><br>
As engineering: the <strong>CVA prognosis publication at IEEE WCONF 2023</strong> — it was the first time I saw the full loop close: clinical question → ML pipeline → peer-reviewed result → software copyright. The model genuinely works and the publication stands behind it.<br><br>
As science: the <strong>gene clustering bioinformatics work</strong>. Starting from 49,000 genes with no clean labels and landing on three immunologically meaningful hub genes — CD4, CXCL10, FMO3 — felt like actual discovery, not just optimisation. That one I'd stake my name on in a room full of biologists.`,
      followups: ["Tell me about your CVA paper", "What are your gene clustering findings?", "How many publications do you have?"]
    },

    // ── PUBLICATION AREAS / TOPICS ────────────────────────────────────────────
    pub_areas: {
      patterns: [/what.*publish.*about|research.*topic|what.*research area|topic.*publish|what.*area.*publish|area.*research/i],
      answer: `Four main threads run through my publications:<br><br>
• <strong>ML for healthcare</strong> — stroke prognosis, fraud detection, malware classification, maternal risk<br>
• <strong>Deep learning for biomedical imaging</strong> — brain tumour segmentation using GlobalNet + FusionNet<br>
• <strong>NLP</strong> — speech segmentation, resume screening, grid computing<br>
• <strong>Bioinformatics</strong> — gene clustering, pathway enrichment, T2D/atherosclerosis intersection<br><br>
The connecting thread is always: complex, high-stakes data where the result matters to a real person — a patient, a clinician, a fraud investigator. I don't publish on toy problems.`,
      followups: ["Tell me about your most important paper", "What's your gene clustering research?", "What conferences do you publish at?"]
    },

    // ── PATENTS & COPYRIGHTS ──────────────────────────────────────────────────
    patents_copyrights: {
      patterns: [/patent|copyright|ip.*regist|software.*copyright|intellectual.*property|co.?author.*patent/i],
      answer: `I hold <strong>2 software copyrights</strong> registered with the Indian Copyright Office — one for the CVA prognosis system. I also co-authored a <strong>UK design patent</strong>.<br><br>
It's unusual for a data scientist at this stage to have IP registrations, but I've always treated the systems I build as things worth protecting — not just submitting to a repo and moving on. If you've built something that genuinely works, it should be documented that you built it.`,
      followups: ["Tell me about your CVA project", "What are your achievements?", "Your publications?"]
    },

    // ── OPEN TO RESEARCH ROLES ────────────────────────────────────────────────
    open_research: {
      patterns: [/open.*research|research.*role|research.*position|industry.*vs.*research|research.*job|both.*research.*industry/i],
      answer: `Both, if the work is right.<br><br>
I have 12+ publications alongside full-time industry work — that's not an accident, that's a habit I've maintained deliberately. The ideal for me is AI/ML engineering at a company that also publishes and contributes to research: applied scientist roles, or companies with serious research arms.<br><br>
I don't want to give up either leg. The research keeps the engineering honest — you ask "why" more often when you've spent time in academic peer review. The engineering keeps the research grounded — you ask "but does it scale?" more often when you've run models on 10M records.`,
      followups: ["What roles are you open to?", "Tell me about your publications", "Would you consider a PhD?"]
    },

    // ── PhD INTEREST ──────────────────────────────────────────────────────────
    phd_interest: {
      patterns: [/phd|ph\.d|doctorate|graduate.*school|grad.*school|do.*phd|consider.*phd/i],
      answer: `I haven't ruled it out.<br><br>
The honest answer: I get most of what I love about research — deep investigation, publishing, peer review — without a PhD program right now. 12+ papers alongside industry work is evidence that I can do the research independently.<br><br>
If there's a specific lab working on biomedical AI at a scale that only a PhD gives you access to — foundation models for clinical data, large-scale genomics — I'd seriously consider it. But I'm not doing a PhD to collect a credential. I'd only do it if the research itself demanded it.`,
      followups: ["What research are you interested in?", "Are you open to research roles?", "Tell me about your publications"]
    },

    // ── CURRENT RESEARCH INTERESTS ────────────────────────────────────────────
    future_research: {
      patterns: [/research.*interest|interested.*research|what.*research.*pursue|current.*research|research.*focus.*next/i],
      answer: `Biomedical AI at the clinical decision-making layer — specifically where foundation models meet structured clinical data.<br><br>
I'm interested in how you build <em>trustworthy</em> AI systems in healthcare: not just accurate, but interpretable and safe enough that a clinician can act on the output without second-guessing the model. The failure modes in clinical AI are genuinely dangerous, and that intersection of reliability engineering and biology is where I want to contribute next.<br><br>
The fraud detection work at Cigna sharpened that instinct — once you've watched your model's output drive real decisions, you never think about uncertainty quantification the same way again.`,
      followups: ["Tell me about your healthcare ML work", "Are you open to research roles?", "What's your current role?"]
    },

    // ── DATABRICKS SPARKWARS ──────────────────────────────────────────────────
    achievement_databricks: {
      patterns: [/databricks|sparkwars|spark.*hackathon|hackathon.*databricks/i],
      answer: `<strong>Top 10 at the Databricks Sparkwars Hackathon</strong>.<br><br>
It was a competitive hackathon around large-scale data engineering and ML on the Databricks platform. Getting into the top 10 out of a large field validated something specific: my engineering instincts hold up under time pressure and competition, not just in research papers where you have unlimited revision cycles.<br><br>
Hackathons are honest about gaps — you either ship something that works or you don't. That one, we did.`,
      followups: ["What other competitions have you done?", "Tell me about your achievements", "Your Walmart Sparkathon experience?"]
    },

    // ── DISS FEST ────────────────────────────────────────────────────────────
    achievement_dissfest: {
      patterns: [/diss.?fest|oral.*presentation.*award|1st prize.*present|first prize.*present|presentation.*award/i],
      answer: `<strong>1st Prize at DISS FEST 2024</strong> — an academic oral presentation competition.<br><br>
The judging is split: quality of research and quality of communication. You're presenting to a mixed audience — technical reviewers and people who just need to follow the argument. That balance is something I've worked hard on. The best research in the room doesn't win if the presenter can't make the stakes clear to someone who didn't come in knowing the domain.<br><br>
That lesson carries directly into how I design dashboards and present model results at work.`,
      followups: ["Tell me about your other achievements", "Your publications and awards?", "How do you communicate technical results?"]
    },

    // ── IEEE WCONF ────────────────────────────────────────────────────────────
    conf_wconf: {
      patterns: [/wconf|world.*conference.*commun|ieee.*world.*conf|kalinga.*univ/i],
      answer: `<strong>IEEE WCONF</strong> — IEEE World Conference on Communication & Computing — is an international IEEE-indexed conference organised by Kalinga University, held in Raipur, India.<br><br>
It covers communication systems, computing, networking, and emerging AI/ML applications. Being IEEE-indexed means it's Scopus-listed and peer-reviewed at international standards.<br><br>
My CVA Prognosis publication appeared at WCONF 2023 in July — and I won the <strong>Best Presentation Award</strong> there for it. That was the same work published alongside a software copyright registration.`,
      followups: ["Tell me about your CVA publication", "What other conferences have you published at?", "Your awards and achievements?"]
    },

    // ── IEEE CSNT ─────────────────────────────────────────────────────────────
    conf_csnt: {
      patterns: [/csnt|communication systems.*network|ieee.*csnt|ieee.*bhopal.*2023/i],
      answer: `<strong>IEEE CSNT</strong> — IEEE International Conference on Communication Systems and Network Technologies — is a well-established IEEE conference. The 12th edition was held in Bhopal, India in 2023.<br><br>
It focuses on communication systems, network technologies, and AI/signal processing applications. IEEE-indexed, Scopus-listed, peer-reviewed.<br><br>
Two of my publications appeared at CSNT 2023:<br>
• <strong>Brain Tumor Segmentation</strong> (GlobalNet + FusionNet) — <strong>Best Paper Award</strong><br>
• <strong>Speech Signal Segmentation</strong> (Kernelized Deep Networks) — <strong>Best Presentation Award</strong>`,
      followups: ["Tell me about your brain tumor publication", "What other IEEE conferences?", "Your best paper awards?"]
    },

    // ── ICICT / SPRINGER ──────────────────────────────────────────────────────
    conf_icict: {
      patterns: [/icict|international congress.*ict|ninth.*congress.*ict|lecture notes.*network/i],
      answer: `<strong>ICICT</strong> — International Congress on Information and Communication Technology — is an annual international forum published by <strong>Springer</strong> in the Lecture Notes in Networks and Systems (LNNS) series, which is Scopus-indexed.<br><br>
My NLP-based resume screening publication appeared in the 9th edition (ICICT 2024): <em>"NLP Based AI-Driven Resume Screening Solution for Efficient Candidate Selection"</em>. It used spaCy NER to extract resume entities and rank candidates — outperforming traditional screening approaches in accuracy and fairness.`,
      followups: ["Tell me about your NLP resume screening work", "What other Springer publications do you have?", "Your full publications list?"]
    },

    // ── ALL PUBLICATIONS FULL LIST ────────────────────────────────────────────
    pub_full_list: {
      patterns: [/list.*all.*pub|all.*pub|full.*list.*pub|complete.*pub|how many.*pub|12.*pub|all.*research.*pub|every.*pub/i],
      answer: `Here are all my 15+ publications across IEEE, Springer, Elsevier, CRC Press, and more:<br><br>
<strong>IEEE (peer-reviewed, Scopus):</strong><br>
• Brain Tumor Segmentation — GlobalNet + FusionNet → <em>IEEE CSNT 2023</em> 🏆 Best Paper Award<br>
• Speech Signal Segmentation — Kernelized Deep Networks → <em>IEEE CSNT 2023</em> 🏆 Best Presentation Award<br>
• CVA Prognosis — Supervised ML → <em>IEEE WCONF 2023</em> 🏆 Best Presentation Award<br>
• Malware Classification — Cuckoo + Boruta + KNN → <em>Procedia Computer Science, ScienceDirect 2024</em><br><br>
<strong>Springer:</strong><br>
• ML in VLSI IC Design → <em>SN Computer Science, 2023</em><br>
• NLP Resume Screening → <em>ICICT 2024 (LNNS)</em><br>
• Smart Wearable for Specially Abled → <em>Springer, 2024</em><br>
• Silver Nanoparticles Functionalisation → <em>Springer Nature (forthcoming)</em><br><br>
<strong>Elsevier:</strong><br>
• Brain Computer Interfaces → <em>ML Models for Biomedical Signal Processing, 2025</em><br>
• Advanced CNN for Brain Tumors → <em>Computers in Biology and Medicine (forthcoming)</em><br><br>
<strong>CRC Press / Other:</strong><br>
• IoT Vulnerability Assessment → <em>CRC Press, 2024</em><br>
• Digital Forensic Intelligence (EdgeAI) → <em>CRC Press, 2024</em><br>
• Bacterial Exopolysaccharides for Biofilm → <em>IGI Global, 2024</em><br>
• Neural Image Caption Generation → <em>IJSRSET, 2023</em><br>
• Fintech Crisis & Marketing → <em>Atlantis Press ICETBM, 2023</em><br>
• Grid Computing + Deep Learning → <em>IJAR, 2023</em><br>
• Memory-Augmented Deep RNN for NLP → <em>(forthcoming)</em><br><br>
Plus 2 software copyrights and a co-authored UK Design Patent. Check the Publications section on this page for direct links.`,
      followups: ["Tell me about your IEEE publications", "Your Springer publications?", "Best paper awards?"]
    },

    // ── BRAIN TUMOR PUBLICATIONS ──────────────────────────────────────────────
    pub_brain_tumor: {
      patterns: [/brain.?tum(o|ou)r|globalnet|fusionnet|segmentation.*tum|tum.*segment|deep.?learn.*tum/i],
      answer: `Two publications on brain tumor work:<br><br>
<strong>1. "Accurate Brain Tumor Segmentation and Detection using Multi-Task Learning with GlobalNet and FusionNet"</strong><br>
<em>IEEE CSNT 2023, Bhopal — 🏆 Best Paper Award</em><br>
Used three complementary deep learning architectures: GlobalNet (global context via pooling), Multi-Task Learning (shared knowledge across concurrent tasks), and FusionNet (combining CNNs trained on different medical imaging modalities). A novel ensemble approach to make tumor segmentation both accurate and robust across scan types.<br><br>
<strong>2. "An Advanced Fully Residual CNN for Segmentation and Classification of Brain Tumors Across Diverse Medical Image Modalities"</strong><br>
<em>Computers in Biology and Medicine — Elsevier (forthcoming)</em><br>
A more advanced follow-up using a fully residual CNN architecture covering multiple imaging modalities — MRI, CT, and others.`,
      followups: ["What was your best paper award for?", "Tell me about your IEEE CSNT publications", "Other healthcare ML publications?"]
    },

    // ── SPEECH + NLP PUBLICATIONS ─────────────────────────────────────────────
    pub_speech_nlp: {
      patterns: [/speech.*segment|kernelized.*deep|speech.*signal.*pub|memory.*augmented.*rnn|recurrent.*nlp.*pub/i],
      answer: `Three NLP/speech publications:<br><br>
<strong>1. "Kernelized Deep Networks for Speech Signal Segmentation Using Clustering and AI in Neural Networks"</strong><br>
<em>IEEE CSNT 2023 — 🏆 Best Presentation Award</em><br>
Applied kernelized deep networks for segmenting speech signals, combining clustering with deep neural networks.<br><br>
<strong>2. "Cluster-Based Grid Computing on Wireless Network Data Transmission With Routing Analysis Protocol and Deep Learning"</strong><br>
<em>International Journal of Advanced Research (IJAR), 2023</em><br>
Optimised wireless data transmission using cluster-based grid computing and intelligent routing via deep learning.<br><br>
<strong>3. "Memory-Augmented Deep Recurrent Neural Networks for Long-Term Dependency Learning in NLP"</strong><br>
<em>Forthcoming</em><br>
Explores memory augmentation for handling long-range dependencies — a known limitation of standard RNNs.`,
      followups: ["Tell me about your IEEE CSNT publications", "Your NLP project work?", "Other publications?"]
    },

    // ── MALWARE CLASSIFICATION PUBLICATION ───────────────────────────────────
    pub_malware: {
      patterns: [/malware.*classif|malware.*pub|cuckoo.*env|boruta.*feature|knn.*malware/i],
      answer: `<strong>"Predicting Malware Classification and Family using Machine Learning: A Cuckoo Environment Approach with Automated Feature Selection"</strong><br>
<em>Procedia Computer Science — ScienceDirect (Elsevier), 2024</em><br><br>
Approach: dynamic malware analysis inside a Cuckoo sandbox environment, with Boruta automated feature selection to identify the most discriminating behavioural features. Classifier: KNN.<br><br>
Performance (Test Case 3 — Automated Feature Selection + Cross-Validation):<br>
• Specificity: 90% &nbsp;• Precision: 93%<br>
• Recall: 96% &nbsp;&nbsp;&nbsp;&nbsp;• F1-Score: 92%<br><br>
Particularly strong on phishing detection — a real-world threat classification problem.`,
      followups: ["Tell me about your other security publications", "IoT vulnerability work?", "Your publications at Elsevier?"]
    },

    // ── NLP RESUME SCREENING PUBLICATION ─────────────────────────────────────
    pub_resume_screening: {
      patterns: [/resume.*screen|screen.*resume|nlp.*resume|candidate.*select.*pub|spacy.*resume/i],
      answer: `<strong>"NLP Based AI-Driven Resume Screening Solution for Efficient Candidate Selection"</strong><br>
<em>Proceedings of ICICT 2024 — Springer Lecture Notes in Networks and Systems (Scopus-indexed)</em><br><br>
Built using spaCy's NER library to extract structured data from free-form resumes: names, organisations, job titles, skills, and education. Pipeline: tokenisation → NER-based scoring → ranked candidate listing.<br><br>
Outperformed traditional rule-based screening in accuracy, efficiency, and fairness — reducing manual effort and eliminating keyword-stuffing vulnerabilities in naive keyword matching.`,
      followups: ["Tell me about your ICICT/Springer publications", "What NLP tools do you use?", "Other NLP publications?"]
    },

    // ── NEURAL IMAGE CAPTION PUBLICATION ─────────────────────────────────────
    pub_neural_caption: {
      patterns: [/neural.*image.*caption|image.*caption.*pub|visual.*attention.*image|visually.*impaired.*pub/i],
      answer: `<strong>"Neural Image Caption Generation with Visual Attention: Enabling Image Accessibility for the Visually Impaired"</strong><br>
<em>IJSRSET, Volume 10, Issue 3 — 2023</em><br><br>
Model architecture: CNN (feature extraction) + RNN with visual attention (caption generation) + text-to-speech API (audio output). Two-phase pipeline that converts image content into spoken descriptions.<br><br>
The goal was practical digital inclusion — making web images and visual content accessible to visually impaired users without requiring manual alt-text. A direct human-impact application of computer vision.`,
      followups: ["Tell me about your computer vision projects", "Other accessibility-focused work?", "Your publications in 2023?"]
    },

    // ── IOT & CYBERSECURITY PUBLICATIONS ─────────────────────────────────────
    pub_iot_security: {
      patterns: [/iot.*vulnerab|digital.*forensic.*pub|edge.*ai.*forensic|cyber.*defence.*pub|iot.*sustainable/i],
      answer: `Two cybersecurity/EdgeAI publications in the same CRC Press volume (<em>Big Data & Edge Intelligence for Enhanced Cyber Defence, 2024</em>):<br><br>
<strong>1. "Challenges, Existing Strategies, and New Barriers in IoT Vulnerability Assessment for Sustainable Computing"</strong><br>
Comprehensive review of IoT security challenges — current strategies, known gaps, and emerging barriers as IoT scales into sustainable infrastructure.<br><br>
<strong>2. "Advancing Digital Forensic Intelligence: Leveraging EdgeAI Techniques for Real-time Threat Detection and Privacy Protection"</strong><br>
Explores how Edge AI — processing at the device level rather than cloud — enhances digital forensics for real-time threat detection while preserving privacy.`,
      followups: ["Tell me about your other cybersecurity work", "Malware classification publication?", "CRC Press publications?"]
    },

    // ── BRAIN COMPUTER INTERFACE PUBLICATION ─────────────────────────────────
    pub_bci: {
      patterns: [/brain.?computer.?interface|bci.*pub|brain.*interface.*pub|elderly.*disabled.*pub/i],
      answer: `<strong>"Brain Computer Interfaces for Elderly and Disabled Person"</strong><br>
<em>Machine Learning Models and Architectures for Biomedical Signal Processing — Elsevier, 2025</em><br><br>
Covers ML-based BCI applications designed for accessibility — specifically for elderly and disabled populations where traditional input methods are unavailable. Explores signal processing architectures that bridge neural activity and machine-interpretable commands.<br><br>
This sits at the intersection of two areas I care about: making ML useful in healthcare, and designing systems for people who need them most.`,
      followups: ["Tell me about your Elsevier publications", "Other biomedical ML publications?", "Your brain tumor work?"]
    },

    // ── BIOFILM / NANOMATERIALS PUBLICATIONS ─────────────────────────────────
    pub_biofilm_nano: {
      patterns: [/bacterial.*exo|exopolysaccharide|biofilm.*nano|silver.*nano.*pub|nanoparticle.*pub|nanomaterial.*pub/i],
      answer: `Two nanomaterials/biotech publications from my biofilm research background:<br><br>
<strong>1. "Bacterial Exopolysaccharides-Based Nanomaterials for Targeting Biofilm-Associated Infections"</strong><br>
<em>Cutting-Edge Applications of Nanomaterials in Biomedical Sciences — IGI Global, 2024</em><br>
Connecting to my wet-lab work at REC's Centre of Excellence in Biofilms — EPS-based nanomaterials as targeted delivery systems against drug-resistant biofilm infections.<br><br>
<strong>2. "Functionalisation Strategies of Silver Nanoparticles"</strong><br>
<em>Springer Nature — forthcoming</em><br>
Surface functionalisation approaches for silver nanoparticles to enhance specificity and biocompatibility in antimicrobial applications.`,
      followups: ["Tell me about your biofilm lab work", "Your biotech background?", "Other publications?"]
    },

    // ── VLSI + FINTECH + WEARABLE PUBLICATIONS ────────────────────────────────
    pub_vlsi_misc: {
      patterns: [/vlsi|integrated.*circuit.*ml|fintech.*crisis|fintech.*marketing.*pub|smart.*wearable.*pub|specially.*abled.*pub/i],
      answer: `Three additional publications across VLSI, fintech, and education tech:<br><br>
<strong>1. "Implementation of Machine Learning in VLSI Integrated Circuit Design"</strong><br>
<em>Springer SN Computer Science, 2023</em><br>
Explores applying ML to optimise VLSI IC design — predicting circuit behaviour and improving design efficiency through learned models.<br><br>
<strong>2. "Fintech, Crisis, and Marketing: How Technology-Driven Financial Firms Adapt Their Approach to Retain Customers"</strong><br>
<em>ICETBM 2023 — Atlantis Press</em><br>
Examines how fintech firms pivot marketing strategy during economic crises — data-driven retention approaches.<br><br>
<strong>3. "Design and Development of Smart Wearable Technology Enhanced Learning for Specially Abled Students"</strong><br>
<em>Perspective and Strategies on Newage Education — Springer, 2024</em><br>
Accessible education technology using smart wearables to support learners with disabilities.`,
      followups: ["Tell me about your Springer publications", "Your full publications list?", "Other interdisciplinary work?"]
    },

    // ── WHY DO YOU BLOG ──────────────────────────────────────────────────────
    blog_why: {
      patterns: [/why.*blog|why.*you write|purpose.*blog|blog.*purpose|what.*motivat.*write|why.*write.*blog/i],
      answer: `Papers are formal, reviewed, and slow. Blogs are where I think out loud.<br><br>
I write to understand — if I can't explain something clearly in plain language, I don't really understand it yet. The T2DM molecular research post forced me to trace the whole signalling pathway before I could write a sentence that made sense. That process made me better at the actual research.<br><br>
It's also a record of what I found interesting at a given point in time. Reading back through old posts is how I trace how my thinking has shifted — which I find genuinely useful.`,
      followups: ["What do you write about on your blog?", "Tell me about your research", "Your publications?"]
    },

    // ── UNEXPECTED TRAIT ─────────────────────────────────────────────────────
    unexpected_trait: {
      patterns: [/don't expect|not expect|people.*expect|unexpected.*about|surpris.*about you|what.*surpris/i],
      answer: `People expect a data person to just be good with numbers. What they don't expect is that I come from biology — I've actually read gene expression papers, worked in a biofilm lab — and that scientific rigour carries over into how I approach ML problems.<br><br>
I don't just tune and ship; I ask what the output means. That cross-domain thinking — biology meeting ML engineering — is probably the most underrated thing about how I work.`,
      followups: ["How did you get from biotech to data science?", "Tell me about your research", "What makes you different from other candidates?"]
    },

    // ── WHAT EXCITES YOU ─────────────────────────────────────────────────────
    excites_you: {
      patterns: [/what.*excit|excit.*you|genuinely.*interest|kind of.*role.*excit|what.*passionate|what.*gets you|what problem.*interest/i],
      answer: `Anything where a model isn't just a demo — where it's plugged into a real decision system with real stakes.<br><br>
Healthcare fraud detection at Cigna was that for me: the model output fed directly into investigator queues. That feedback loop between ML output and human action is what gets me.<br><br>
Longer term I want to work on AI engineering problems — building and deploying intelligent systems at scale. Not notebooks. Systems.`,
      followups: ["Tell me about your Cigna work", "What roles are you open to?", "Where do you see yourself in 5 years?"]
    },

    // ── WHY STUDY BIOTECHNOLOGY ───────────────────────────────────────────────
    why_biotech_choice: {
      patterns: [/why.*study.*bio|why.*chose.*bio|biotech.*if.*wanted|chose.*biotech|wrong.*field|different.*degree.*data/i],
      answer: `Biology taught me something a CS degree alone wouldn't have — how to think about messy, high-dimensional, noisy data where ground truth is genuinely hard to establish.<br><br>
When I worked on gene expression data with 49,000 features and no clean label, I understood <em>biologically</em> why the noise was there. That context made me a better ML practitioner, not a worse one.<br><br>
I didn't choose the wrong field — I chose a field that gave me an unusual foundation for the field I'm moving into.`,
      followups: ["Tell me about your gene clustering project", "How did you get into ML?", "What's your edge as a data scientist?"]
    },

    // ── AREN'T YOU JUST JUMPING AROUND ───────────────────────────────────────
    zigzag: {
      patterns: [/jumping around|zigzag|scattered|too many.*field|what.*actually want|not focused|why.*so many/i],
      answer: `The thread has always been the same: I want to work on problems where the data is complex, the stakes are real, and the answer actually matters to someone.<br><br>
In biotech that was gene expression and disease pathways. In data science that was fraud detection and clinical risk prediction. In AI engineering, that's building the systems that take those models and make them reliable, scalable, and deployable.<br><br>
It's not a zigzag — it's a narrowing. Each step I've gotten closer to the intersection I want to own: intelligent systems built on biological and healthcare data.`,
      followups: ["What kind of role are you looking for?", "Tell me about your Cigna experience", "Where do you see yourself in 5 years?"]
    },

    // ── IS BIOLOGY A DISADVANTAGE IN TECH ────────────────────────────────────
    biology_disadvantage: {
      patterns: [/bio.*disadvantage|disadvantage.*bio|pure.*tech.*bio|bio.*tech.*role|only cs|pure cs.*better/i],
      answer: `I think it's the opposite.<br><br>
Most ML engineers haven't sat with a clinician trying to explain why a model flagged a patient as high risk. Most haven't had to justify a clustering result to someone who knows what CD4 expression actually means physiologically. That cross-domain fluency is genuinely rare.<br><br>
In healthcare AI — where some of the most important and underdeveloped ML work is happening — it's a real edge. I'm not a biologist who learned to code. I'm an AI engineer who understands the domain deeply enough to not break things in ways that matter.`,
      followups: ["Why should we hire you?", "Tell me about your healthcare ML work", "What's your gene clustering project?"]
    },

    // ── WHY AI ENGINEERING SPECIFICALLY ──────────────────────────────────────
    why_ai_engineering: {
      patterns: [/why.*ai.*engineer|ai engineer.*not|not.*research.*ai|why engineer.*not research|research.*vs.*engineer/i],
      answer: `Because I've seen what happens when a good model doesn't make it past a notebook.<br><br>
At Cigna, the thing that mattered wasn't the accuracy number — it was whether the fraud investigator could act on the output. That gap between model and impact is an engineering problem. I want to close that gap.<br><br>
Research is great and I'll always have one foot there — I have 12+ publications — but I want to build things that run, scale, and actually do something in the world. AI engineering sits right at that junction.`,
      followups: ["Tell me about your Cigna work", "What roles are you open to?", "Your research publications?"]
    },

    // ── BEHAVIORAL — CONFLICT / DISAGREEMENT ─────────────────────────────────
    behavior_conflict: {
      patterns: [/conflict.*team|disagree.*team|clash|co.?author.*disagree|push.*back.*stakeholder|difference.*opinion/i],
      answer: `During a research project, I had a difference of opinion with a co-author on which model to present — they wanted to lead with a simpler model for accessibility, I thought the more complex one told a more honest story about the data.<br><br>
We resolved it by presenting both, with a clear explanation of the tradeoffs. I think disagreement is healthy when both people are arguing from data and not ego. Lead with evidence, not preference — that's a habit I try to maintain.`,
      followups: ["How do you work with stakeholders?", "Tell me about communicating technical results", "Your team collaboration style?"]
    },

    // ── CHESS AND PROFESSIONAL THINKING ──────────────────────────────────────
    chess_influence: {
      patterns: [/chess.*work|chess.*professional|chess.*think|chess.*influence|how.*chess.*ml|chess.*data|chess.*eng/i],
      answer: `Chess trained me to think a few moves ahead and to not fall in love with my first idea.<br><br>
In ML work that shows up as: before I commit to a modelling approach, I map out what could go wrong — data drift, label imbalance, deployment constraints. It's the same instinct as not playing the obvious move without checking what it opens up for the opponent.<br><br>
The other thing chess teaches is that the post-game analysis after a loss is as valuable as the game itself. I bring that same post-mortem instinct to failed experiments and false starts in ML.`,
      followups: ["What are your hobbies outside work?", "Tell me about your problem-solving approach", "What drives you professionally?"]
    },

    // ── CROSS-DOMAIN EXPLORATION ──────────────────────────────────────────────
    cross_domain_explore: {
      patterns: [/cross.?domain|borrow.*solution|explore.*professionally|cross.*field|multi.*domain|t.?shaped|range.*expertise/i],
      answer: `Going from biotech to bioinformatics to data science to AI engineering — that <em>is</em> the professional version of loving to explore.<br><br>
But within projects too: I've worked across fraud detection, Bayesian marketing models, NLP, gene expression, computer vision — not because I was scattered, but because every domain taught me something the others didn't.<br><br>
The Bayesian uncertainty quantification I use in MMM comes from the same probabilistic thinking I applied to gene clustering validation. You borrow solutions from unexpected places when you've worked across enough domains.`,
      followups: ["Tell me about your different projects", "Why did you switch from biotech?", "What's your approach to learning new things?"]
    },

    // ── FALLBACK ─────────────────────────────────────────────────────────
    fallback: [
      `Good question — I might not have that detail here. Reach me at <a href="mailto:${EMAIL}">${EMAIL}</a> and I'll give you a proper answer.`,
      `That's a bit outside what I have in my knowledge base right now. Drop me an email at <a href="mailto:${EMAIL}">${EMAIL}</a> — I'll respond directly.`,
      `I don't have the exact answer for that here, but feel free to <a href="mailto:${EMAIL}">email me ↗</a> or explore my <a href="${GH}?tab=repositories" target="_blank">GitHub ↗</a>.`,
    ]
  };

  // ── TEASER MESSAGES ───────────────────────────────────────────────────────
  const TEASER_MSGS = [
    "👋 Ask me anything — I'll answer directly!",
    "💼 Want to know about my projects?",
    "🔬 Curious about my research?",
    "✉️ Looking to hire? Let's talk!",
    "🤖 Ask me about my tech stack!",
    "📄 Want to see my resume?",
    "🧬 Ask about my bioinformatics work!",
  ];

  // ── DEFAULT CHIPS (second-person) ─────────────────────────────────────────
  const DEFAULT_SUGGESTIONS = [
    "Tell me about yourself",
    "What projects have you built?",
    "What's your current role?",
    "What's your tech stack?",
    "What research have you done?",
    "How can I contact you?",
  ];

  // ── INTENT ORDER (specific → general) ────────────────────────────────────
  const INTENT_ORDER = [
    'currently_doing','company_cigna','current_role',
    'company_crayon','crayon',
    'company_quantalytics',
    'why_switch','why_biotech_choice','zigzag','biology_disadvantage','why_ai_engineering',
    'proud_project','why_hire','weakness','unexpected_trait','excites_you',
    'behavior_ambiguous','behavior_conflict','behavior_learning','behavior_failure','behavior_communication',
    'future','open_research','phd_interest','future_research','availability',
    'chess_influence','cross_domain_explore','personal',
    'project_stroke','project_gene','project_fraud',
    'project_logs','project_paper','project_mmm','project_smart_city','project_etl',
    'project_banking','project_face','project_scheduler','project_heart',
    'project_fakenews','project_ads','project_maternal','project_image',
    'projects_overview','experience',
    'course_iitg','university_rec','education',
    'conf_wconf','conf_csnt','conf_icict',
    'pub_brain_tumor','pub_speech_nlp','pub_malware','pub_resume_screening',
    'pub_neural_caption','pub_iot_security','pub_bci','pub_biofilm_nano','pub_vlsi_misc',
    'pub_full_list','pub_important','pub_areas','patents_copyrights','publications',
    'skills','achievement_databricks','achievement_dissfest','achievements',
    'domain_atherosclerosis','domain_t2d','research',
    'contact','resume','biotech','personality',
    'blog_why','blog','location','thanks','github_all','identity'
  ];

  function getResponse(text) {
    for (const key of INTENT_ORDER) {
      const item = KB[key];
      if (item && item.patterns && item.patterns.some(p => p.test(text))) {
        return item;
      }
    }
    return {
      answer: KB.fallback[Math.floor(Math.random() * KB.fallback.length)],
      followups: ["Tell me about your projects", "What's your experience?", "How to contact you?"]
    };
  }

  // ── BUILD DOM ─────────────────────────────────────────────────────────────
  const markup = `
  <div id="cb-teaser" aria-hidden="true">
    <div class="cb-teaser-bubble"><span class="cb-teaser-text"></span></div>
    <div class="cb-teaser-arrow"></div>
  </div>

  <button id="cb-launcher" aria-label="Chat with Niveditha">
    <span class="cb-launcher-icon" id="cb-launcher-icon"><i class="fa-solid fa-circle-question"></i></span>
  </button>

  <div id="cb-window" role="dialog" aria-label="Chat with Niveditha Srikanth">
    <div class="cb-header">
      <div class="cb-avatar">
        <img src="images/favicon.png" alt="Niveditha"
          onerror="this.src='images/niveditha.gif';this.onerror=null;">
      </div>
      <div class="cb-header-info">
        <div class="cb-header-name">Niveditha Srikanth</div>
        <div class="cb-header-status">
          <div class="cb-status-dot"></div>
          Talking directly to Nivi
        </div>
      </div>
      <div class="cb-header-actions">
        <button class="cb-header-btn" id="cb-tts-toggle" title="Toggle voice"><i class="fa-solid fa-volume-xmark"></i></button>
        <button class="cb-header-btn" id="cb-close" title="Close">✕</button>
      </div>
    </div>
    <div class="cb-messages" id="cb-messages"></div>
    <div class="cb-suggestions" id="cb-suggestions"></div>
    <div class="cb-input-bar">
      <textarea id="cb-input" placeholder="Ask Niveditha anything…" rows="1"></textarea>
      <button class="cb-btn" id="cb-mic" title="Voice input"><i class="fa-solid fa-microphone-slash"></i></button>
      <button class="cb-btn" id="cb-send" title="Send">➤</button>
    </div>
  </div>`;

  const root = document.createElement('div');
  root.id = 'cb-root';
  root.innerHTML = markup;
  document.body.appendChild(root);

  const launcher   = document.getElementById('cb-launcher');
  const cbWindow   = document.getElementById('cb-window');
  const messages   = document.getElementById('cb-messages');
  const inputEl    = document.getElementById('cb-input');
  const sendBtn    = document.getElementById('cb-send');
  const micBtn     = document.getElementById('cb-mic');
  const closeBtn   = document.getElementById('cb-close');
  const ttsToggle  = document.getElementById('cb-tts-toggle');
  const suggestBox = document.getElementById('cb-suggestions');
  const teaser     = document.getElementById('cb-teaser');
  const teaserText = teaser.querySelector('.cb-teaser-text');

  let isOpen = false, ttsOn = false, recognition = null;
  let listening = false, greeted = false;
  let teaserActive = false, teaserIndex = 0, teaserTimer = null;

  // ── SCROLL REVEAL ─────────────────────────────────────────────────────────
  function checkScroll() {
    if (window.scrollY > 180) {
      launcher.classList.add('cb-visible');
      if (!isOpen && !greeted) startTeaser();
    } else {
      launcher.classList.remove('cb-visible');
      stopTeaser();
    }
  }
  window.addEventListener('scroll', checkScroll, { passive: true });
  checkScroll();

  // ── TEASER ────────────────────────────────────────────────────────────────
  function startTeaser() {
    if (teaserActive || isOpen) return;
    teaserActive = true;
    showNextTeaser();
  }
  function stopTeaser() {
    teaserActive = false;
    clearTimeout(teaserTimer);
    teaser.classList.remove('cb-teaser-show');
  }
  function showNextTeaser() {
    if (!teaserActive || isOpen) return;
    const msg = TEASER_MSGS[teaserIndex % TEASER_MSGS.length];
    teaserIndex++;
    teaserText.textContent = '';
    teaser.classList.add('cb-teaser-show');
    let i = 0;
    const t = setInterval(() => {
      if (i < msg.length) { teaserText.textContent += msg[i++]; }
      else {
        clearInterval(t);
        teaserTimer = setTimeout(() => {
          teaser.classList.remove('cb-teaser-show');
          teaserTimer = setTimeout(() => { if (teaserActive && !isOpen) showNextTeaser(); }, 600);
        }, 2800);
      }
    }, 36);
  }

  // ── TTS ───────────────────────────────────────────────────────────────────
  function speak(text) {
    if (!ttsOn || !window.speechSynthesis) return;
    window.speechSynthesis.cancel();
    const plain = text.replace(/<[^>]+>/g,'').replace(/[↗•📂✉️🔬💼🤖📄🧬]/g,'');
    const utt = new SpeechSynthesisUtterance(plain);
    utt.rate = 1.0; utt.pitch = 1.05; utt.lang = 'en-US';
    const voices = window.speechSynthesis.getVoices();
    const female = voices.find(v => /samantha|victoria|karen|moira|ava|allison|zira/i.test(v.name));
    if (female) utt.voice = female;
    window.speechSynthesis.speak(utt);
  }
  ttsToggle.addEventListener('click', () => {
    ttsOn = !ttsOn;
    ttsToggle.innerHTML = ttsOn ? '<i class="fa-solid fa-volume-high"></i>' : '<i class="fa-solid fa-volume-xmark"></i>';
    if (!ttsOn) window.speechSynthesis && window.speechSynthesis.cancel();
  });

  // ── STT ───────────────────────────────────────────────────────────────────
  function setupMic() {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) { micBtn.style.display = 'none'; return; }
    recognition = new SR();
    recognition.lang = 'en-US'; recognition.interimResults = false;
    recognition.onresult = e => { inputEl.value = e.results[0][0].transcript; stopListening(); sendMessage(); };
    recognition.onend = () => stopListening();
    recognition.onerror = () => stopListening();
  }
  function startListening() { if (!recognition) return; listening = true; micBtn.classList.add('cb-listening'); micBtn.innerHTML = '<i class="fa-solid fa-microphone"></i>'; recognition.start(); }
  function stopListening() { listening = false; micBtn.classList.remove('cb-listening'); micBtn.innerHTML = '<i class="fa-solid fa-microphone-slash"></i>'; try { recognition && recognition.stop(); } catch(_){} }
  micBtn.addEventListener('click', () => listening ? stopListening() : startListening());

  // ── MESSAGES ─────────────────────────────────────────────────────────────
  function addMsg(html, role) {
    const msg = document.createElement('div');
    msg.className = `cb-msg cb-${role}`;
    const av = document.createElement('div');
    av.className = 'cb-msg-avatar';
    if (role === 'bot') {
      av.innerHTML = `<img src="images/favicon.png" alt="N" onerror="this.src='images/niveditha.gif';this.onerror=function(){this.parentElement.textContent='NS'}">`;
    } else { av.textContent = '👤'; }
    const bubble = document.createElement('div');
    bubble.className = 'cb-bubble';
    bubble.innerHTML = html;
    msg.appendChild(av); msg.appendChild(bubble);
    messages.appendChild(msg);
    messages.scrollTop = messages.scrollHeight;
  }
  function showTyping() {
    const t = document.createElement('div');
    t.className = 'cb-msg cb-bot cb-typing'; t.id = 'cb-typing';
    const av = document.createElement('div'); av.className = 'cb-msg-avatar';
    av.innerHTML = `<img src="images/favicon.png" alt="N" onerror="this.parentElement.textContent='NS'">`;
    const b = document.createElement('div'); b.className = 'cb-bubble';
    b.innerHTML = `<span class="cb-dot"></span><span class="cb-dot"></span><span class="cb-dot"></span>`;
    t.appendChild(av); t.appendChild(b); messages.appendChild(t);
    messages.scrollTop = messages.scrollHeight;
  }
  function removeTyping() { const t = document.getElementById('cb-typing'); if(t) t.remove(); }

  // ── CHIPS ─────────────────────────────────────────────────────────────────
  function showChips(chips) {
    suggestBox.innerHTML = '';
    (chips || DEFAULT_SUGGESTIONS).forEach(s => {
      const c = document.createElement('button');
      c.className = 'cb-chip'; c.textContent = s;
      c.addEventListener('click', () => { inputEl.value = s; sendMessage(); });
      suggestBox.appendChild(c);
    });
  }

  // ── SEND ─────────────────────────────────────────────────────────────────
  function sendMessage() {
    const text = inputEl.value.trim();
    if (!text) return;
    suggestBox.innerHTML = '';
    addMsg(text, 'user');
    inputEl.value = ''; inputEl.style.height = 'auto';
    const resp = getResponse(text);
    showTyping();
    const delay = 600 + Math.min(resp.answer.replace(/<[^>]+>/g,'').length * 1.2, 1400);
    setTimeout(() => {
      removeTyping();
      addMsg(resp.answer, 'bot');
      speak(resp.answer);
      if (resp.followups) showChips(resp.followups);
    }, delay);
  }
  sendBtn.addEventListener('click', sendMessage);
  inputEl.addEventListener('keydown', e => { if(e.key==='Enter' && !e.shiftKey){e.preventDefault();sendMessage();} });
  inputEl.addEventListener('input', () => { inputEl.style.height='auto'; inputEl.style.height=Math.min(inputEl.scrollHeight,72)+'px'; });

  // ── OPEN / CLOSE ─────────────────────────────────────────────────────────
  function openChat() {
    isOpen = true; stopTeaser();
    cbWindow.classList.add('cb-open'); launcher.classList.add('cb-open');
    document.getElementById('cb-launcher-icon').innerHTML = '<i class="fa-solid fa-xmark"></i>';
    inputEl.focus();
    if (!greeted) {
      greeted = true;
      const g = KB.greeting[Math.floor(Math.random() * KB.greeting.length)];
      setTimeout(() => { addMsg(g, 'bot'); speak(g); showChips(DEFAULT_SUGGESTIONS); }, 320);
    }
  }
  function closeChat() {
    isOpen = false;
    cbWindow.classList.remove('cb-open'); launcher.classList.remove('cb-open');
    document.getElementById('cb-launcher-icon').innerHTML = '<i class="fa-solid fa-circle-question"></i>';
    if (ttsOn) window.speechSynthesis && window.speechSynthesis.cancel();
    setTimeout(() => { if (!isOpen) startTeaser(); }, 5000);
  }
  launcher.addEventListener('click', () => isOpen ? closeChat() : openChat());
  closeBtn.addEventListener('click', closeChat);

  setupMic();
  if (window.speechSynthesis) {
    window.speechSynthesis.getVoices();
    window.speechSynthesis.onvoiceschanged = () => window.speechSynthesis.getVoices();
  }
})();
