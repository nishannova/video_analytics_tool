import cv2
import numpy as np

im = cv2.imread("/Users/sali115/Video_Analytics_Tool_copy/data/processed/temp_frames/frame2.jpg", 0) # read as gray scale
blurred = cv2.GaussianBlur(im, (7, 7), 1.166) # apply gaussian blur to the image
blurred_sq = blurred * blurred
sigma = cv2.GaussianBlur(im * im, (7, 7), 1.166)
sigma = (sigma - blurred_sq) ** 0.5
sigma = sigma + 1.0/255 # to make sure the denominator doesn't give DivideByZero Exception
structdis = (im - blurred)/sigma # final MSCN(i, j) image

# indices to calculate pair-wise products (H, V, D1, D2)
shifts = [[0,1], [1,0], [1,1], [-1,1]]
# calculate pairwise components in each orientation
for itr_shift in range(1, len(shifts) + 1):
    OrigArr = structdis
    reqshift = shifts[itr_shift-1] # shifting index

    for i in range(structdis.shape[0]):
        for j in range(structdis.shape[1]):
            if(i + reqshift[0] >= 0 and i + reqshift[0] < structdis.shape[0] \ and j + reqshift[1] >= 0 and j  + reqshift[1] < structdis.shape[1]):
               ShiftArr[i, j] = OrigArr[i + reqshift[0], j + reqshift[1]]
            else:
               ShiftArr[i, j] = 0

# create affine matrix (to shift the image)
M = np.float32([[1, 0, reqshift[1]], [0, 1, reqshift[0]]])
ShiftArr = cv2.warpAffine(OrigArr, M, (structdis.shape[1], structdis.shape[0]))

# load the model from allmodel file
model = svmutil.svm_load_model("allmodel")
# create svm node array from features list
x, idx = gen_svm_nodearray(x[1:], isKernel=(model.param.kernel_type == PRECOMPUTED))
nr_classifier = 1 # fixed for svm type as EPSILON_SVR (regression)
prob_estimates = (c_double * nr_classifier)()

# predict quality score of an image using libsvm module
qualityscore = svmutil.libsvm.svm_predict_probability(model, x, dec_values)

