# Identity_Fraud_Detection
The repository is created for a fraud analytics project focusing on using supervised machine learning algorithms to detect identity frauds in applicants

## Project Overview
This report examines the Application dataset to detect the fraud activity using supervised machine learning methods. Out team used R as the major tool, “Kolmogorov-Smirnov” as the main feature selection method, and Support Vector Machine, Gradient Boosting Decision Trees, Logistic Regression and Neural Network as featured algorithms to build the predictive model.

## Analysis Description
The steps we took were:
1. Variable construction and data cleaning
2. Feature selection using “Kolmogorov-Smirnov” test
3. Fraud detection model building using SVM, Gradient Boosting Decision Trees, Logistic Regression and Neural Network

The original dataset contains 94,866 records from 1/1/2016 to 12/31/2016 with details about the applicants’ personal information including name, SSN, date of birth, home phone, address, and zip code.

First, we built 160 expert variables with looking at similar characteristics or/and time windows.

Then, we separated the dataset into training, testing and out-of-time dataset, and performed feature selection on the training set. 

In feature selection, we ranked all variables based on the “Kolmogorov-Smirnov” score and selected 30 most important variables. We used the training dataset to train our model first. 

Finally, we tested the model on both training dataset and testing dataset, and built the final model using both training and testing dataset and tested the model on the out-of-time dataset to calculate and compare the final fraud detection rate (FDR).

Using the 30 variables and supervised machine learning algorithms, we found that Gradient Boosting Decision Trees has the best performance, with FDR of 16.94% at 10% of the population.
