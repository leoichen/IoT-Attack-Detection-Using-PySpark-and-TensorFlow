# IoT Attack Detection with Machine Learning

This project attempts to develop ML algorithms using PyTorch and TensorFlow to detect if message is an attack given raw data from 8 IoT sensors that use the MQTT protocol.

The attack types are: ['slowite', 'bruteforce', 'flood', 'malformed', 'dos', 'legitimate'], with 'legitimate' specifying it is a normal message.

Video Demo: https://cmu.app.box.com/s/9ucesmcsf6pfcqenj2aupsc3j2gw8vf0

<br>

## **Files Setup**
The projects asks for the code to be run locally, on the cloud, and on the docker. So, there is a folder for each containing the code that should be run for each platform.

In each folder, there are 3 files:

1) Task 1 and 2.ipynb
2) Twitter Streaming.ipynb
3) Task 3 and 4.ipynb

Task 1 and 2.ipynb and Twitter Streaming.ipynb should be used together to run all the tasks from Task 1 and 2. Task 3 and 4.ipynb consists of the code to complete the tasks in Task 3 and 4. This file also contains Task 1 (building and populating tables), so can be run on its own.

<br>

## **Features**
There are a total of 34 features. The features are attributes of a MQTT protocol message, and some of a TCP message protocol.

|Feature|Description|
| ------------- | ------------- |
|tcp.flags|TCP flag|
|tcp.time_delta|Time of TCP Stream|
|tcp.len|Length of TCP Segment|
|mqtt.conack.flags|Acknowlege Flags|
|mqtt.conack.flags.reserved|Reserved|
|mqtt.conack.flags.sp|Session Present|
|mqtt.conack.val|Return Code|
|mqtt.conflag.cleansess|Clean Session Flag|
|mqtt.conflag.passwd|Password Flag|
|mqtt.conflag.qos|QoS Level|
|mqtt.conflag.reserved|(Reserved)|
|mqtt.conflag.retain|Will Retain|
|mqtt.conflag.uname|User Name Flag|
|mqtt.conflag.willflag|Will Flag|
|mqtt.conflags|Connect Flags|
|mqtt.dupflag|DUP Flag|
|mqtt.hdrflags|Header Flags|
|mqtt.kalive|Keep Alive|
|mqtt.len|Message Length|
|mqtt.msg|Message|
|mqtt.msgid|Message ID|
|mqtt.msgtype|Message Type|
|mqtt.proto_len|Protocol Name Length|
|mqtt.protoname|Protocol Name|
|mqtt.qos|Qos Level|
|mqtt.retain|Retain|
|mqtt.sub.qos|Requested QoS|
|mqtt.suback.qos|Granted QoS|
|mqtt.ver|Version|
|mqtt.willmsg|Will Message|
|mqtt.willmsg_len|Will Message Length|
|mqtt.willtopic|Will Topic|
|mqtt.willtopic_len|Will Topic Length|

<br>

## **Constraints**
The constraints on the features are either on data type or on range of valid values.  

If the constraint type is **boolean**, the data type of the feature is **float** with a range of **[0,1]**.

|Feature|Constraints|
| ------------- | ------------- |
|tcp.flags|String|
|tcp.time_delta|Float|
|tcp.len|Float|
|mqtt.conack.flags|Float|
|mqtt.conack.flags.reserved|Boolean|
|mqtt.conack.flags.sp|Boolean|
|mqtt.conack.val|Float|
|mqtt.conflag.cleansess|Boolean|
|mqtt.conflag.passwd|Boolean|
|mqtt.conflag.qos|Float|
|mqtt.conflag.reserved|Boolean|
|mqtt.conflag.retain|Boolean|
|mqtt.conflag.uname|Boolean|
|mqtt.conflag.willflag|Boolean|
|mqtt.conflags|Float|
|mqtt.dupflag|Boolean|
|mqtt.hdrflags|Float|
|mqtt.kalive|Float|
|mqtt.len|Float|
|mqtt.msg|Float|
|mqtt.msgid|Float|
|mqtt.msgtype|Float|
|mqtt.proto_len|Float|
|mqtt.protoname|String|
|mqtt.qos|Float|
|mqtt.retain|Boolean|
|mqtt.sub.qos|Float|
|mqtt.suback.qos|Float|
|mqtt.ver|Float|
|mqtt.willmsg|Float|
|mqtt.willmsg_len|Float|
|mqtt.willtopic|Float|
|mqtt.willtopic_len|Float|

<br>

## **Feature Engineering**
Data cleaning was done to prepare it for input into machine learning models. The steps taken:

1) Remove all-zero columns
2) Remove absurd columns
3) Remove highly correlated columns
4) Add a data_category column to differentiate test and train data
5) One-Hot Encode nominal columns
6) Vectoize & scale all columns

<br>

**1) Remove all-zero columns**<br>
The following 15 columns only have zeros, so were removed:
- mqtt.conack_flags
- mqtt.conack_flags_reserved
- mqtt.conack_flags_sp
- mqtt.conack_val
- mqtt.conflag_qos
- mqtt.conflag_reserved
- mqtt.conflag_retain
- mqtt.conflag_willflag
- mqtt.retain
- mqtt.sub_qos
- mqtt.suback_qos
- mqtt.willmsg
- mqtt.willmsg_len
- mqtt.willtopic
- mqtt.willtopic_len

<br>

**2) Remove absurd columns**<br>
mqtt.msg was removed because the data doesn't match sense as it has a absurd range of values (values of 4.333e207). Since, mqtt.msgid is linked to mqtt.msg, it was removed too.

<br>

**3) Remove highly correlated columns**<br>
Correlated Groups:

1. 
- mqtt.conflag_passwd
- mqtt.conflag_uname

2.
- mqtt.len
- mqtt.qos

3.
- mqtt.conflag_cleansess
- mqtt.proto_len
- mqtt.ver

The above groups have correlation coefficients of greater than 0.98 within each group. The first column of each group listed above will be kept. So, the below columns will be removed accordingly:

- mqtt.conflag_uname
- mqtt.qos
- mqtt.proto_len
- mqtt.ver

<br>

**4) Add a data_category column to differentiate test and train data**
A new column to differenate between test and train data is added to the combined dataset. This is done so the combined dataset can be read into PostgreSQL without losing the test/train categorization. However, once the data read back in and split between "train" and "test, this column will no longer be needed. So, this column will not be used to train the model.


<br><br>
So, after data cleaning, there are **12 features** left.


<br>

## **Getting It to Run on Docker**
To get it to run on docker, take the following steps:

1) Download the Docker App on your laptop and Open
2) Download the jupyter/pyspark-notebook image by using this command in Terminal: docker pull jupyter/pyspark-notebook
3) Run in Terminal this command to pull the specific image: docker run -p 10000:8888 jupyter/pyspark-notebook:85f615d5cafa
4) Once the pull is command, find the URL in Terminal that looks like: http://127.0.0.1:8888/lab?token=0590f414c98bbe8d07192d6cd7e8e0f1f160ccf33aec3f94
5) Change the 8888 to 10000, so it looks like http://127.0.0.1:10000/lab?token=0590f414c98bbe8d07192d6cd7e8e0f1f160ccf33aec3f94
6) Enter this URL in your browser
  - If it asks for a token, extract it from the URL above. In this case, the token would be: 0590f414c98bbe8d07192d6cd7e8e0f1f160ccf33aec3f94
7) Once the Jupyter notebook is open on Docker, upload:
  - "Task 1 and 2 DOCKER.ipynb"
  - "Twitter Streaming DOCKER.ipynb"
  - "Task 3 and 4 DOCKER.ipynb"
  - the train and test file into a newly created folder called "data_folder"
  - postgresql-42.5.0.jar
8) Follow the steps in the notebook to install all necessary packages

<br>

## **Results and Discussion**

*The results used are extracted from the local version of this project.

**PySparkML** <br>
For PySparkML, the two classifiers chosen are Logistic Regression (LR) and Random Forest (RF). These two were chosen because I wanted to see how an algorithm designed for binary classification (LR) would fare against a algorithm designed for multiclass classifcation (RF). 

From standard training, the accuracies I got were:

- LR
  - Train Accuracy: 0.822
  - Test Accuracy: 0.822
- RF
  - Train Accuracy: 0.836
  - Test Accuracy: 0.848

From the results, RF performed better, but only slightly better. This shows there is not much difference between using LR and RF for multiclass classification. So, the algorithm with higer computational efficiency should be chosen.

To further this point, we need to ensure all variables that affect the accuracies are considered. The most impactful variables are evidently the hyperparameters. So, hyperparameter tuning should be done to find the hyperparameters that give the highest accuracies.

For LR, the hyperparameters tuned are the regularization parameter and # of max iterations. Regularization helps reduce dimensions, and # of max iterations provides more "steps" to gradiently descent, minimizing loss. The values chosen for the regularization parameter are 0.0001/1.0 because I wanted to see the effect of having not having(0.0001) and having (1.0) regularization. The values chosen for max # of interations are 10/50 just to see if a greater number will indeed increase accuracy in this case.

For RF, the hyperparameters tuned are max depth(10/15) and # of trees (30/60). These are the two most impactful hyperparameters for RF, so I wanted to see how changes in these would influence the results.

The test accuracies are:
- LR
  - Standard Training: 0.822
  - Hyperparameter Tuning: 0.827
- RF
  - Standard Training: 0.848
  - Hyperparameter Tuning: 0.892

No difference is seen for LR. For hyperparameters can be done to see if a better model is found. But, given these results, it shows that the best LR can do is a 0.822 accuracy. But, with RF, we see a substantial increase in test accuracy as it goes up to 0.892. This is a 5% bump from the standard training, and 7% higer than that of LR. Assuming this is the best RF model possible, this proves that RF is a better model for multiclass classification.

<br>

**TensorFlow** <br>
For TensorFlow, the two classifiers chosen are a shallow NN (2 hidden layers) and a deep NN (5 hidden layers). I want to see if more hidden layers provides results worth its additional computational cost.

The test accuracies (epoch and width = 10, activation function = relu) are:
- Shallow NN: 0.827
- Deep NN: 0.823

We don't see a difference in the test accuracies. To substantiate these results, cross-validation is done. The results are:
- Shallow NN: 0.826
- Deep NN: 0.814


To further substantiate these results, the effects are hyperparameters is considered. So,tuning is done. The parameters chosen are the width (10/20), chosen to see if more variables will increase accuracy, and the activation function (relu/softmax), chosen to if a different activation function will result in better results. 
- Shallow NN: 0.821
- Deep NN: 0.827

Given all the results, it is proven that the additional hidden layers do not help improve accuracy.
