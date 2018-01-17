---
layout: post
title:  Blog Corpus Industry Mention
date:   2017-10-15
categories: big-data
description: A spark implementation to count the number of times industries have been mentioned in the Blog Corpus dataset in a month of a year. [Dataset](http://u.cs.biu.ac.il/~koppel/BlogCorpus.htm)
comments: true
repo-link: https://github.com/samanta-anupam/big_data_assignments/blob/master/BlogCorpus_industry_count.ipynb
repo-name: Blog Corpus Analysis
---

---
## Table of contents
1. [Goal](#goal)
2. [Dataset](#dataset)
3. [Getting all Industry Names](#industry-names)
    1. [Analysing the task](#analyse-task)
    2. [Broadcasting set of Industries](#broadcast-industry)
4. [Counting Industry mentions over months](#count-industry)
    1. [Analysing the file content](#analyse-file)
    2. [Cleaning file content](#clean-file)
    3. [Counting Industry and the dates when they were mentioned](#count-month)
    4. [Applying on Spark](#apply-spark)
5. [Results](#results)

## 1. <a name="goal"></a>Goal
---

The goal of this post is to use the Blog Corpus dataset and count the number of times a set of industries mentioned in the blogs in each month using spark. In the dataset, the bloggers are classified to various industries, and we have to use these industries and find count of mentions of these industry in all blogs each month. 

## 2. <a name="dataset"></a>Dataset
---

First step would be to download the blog dataset from this [link](http://u.cs.biu.ac.il/~koppel/BlogCorpus.htm). After unzipping the downloaded file, we can see that each file in the corpus is named according to information about the blogger: user_id.gender.age.industry.star_sign.xml

As per the [Blog Corpus Dataset website](http://u.cs.biu.ac.il/~koppel/BlogCorpus.htms):

```
The Blog Authorship Corpus consists of the collected posts of 19,320 bloggers gathered from blogger.com in August 2004. 

The corpus incorporates a total of 681,288 posts and over 140 million words - or approximately 35 posts and 7250 words per person.  

Each blog is presented as a separate file, the name of which indicates a blogger id# and the blogger’s self-provided gender, age, industry and astrological sign. (All are labeled for gender and age but for many, industry and/or sign is marked as unknown.)

All bloggers included in the corpus fall into one of three age groups:

·          8240 "10s" blogs (ages 13-17),

·          8086 "20s" blogs(ages 23-27)

·          2994 "30s" blogs (ages 33-47).

For each age group there are an equal number of male and female bloggers.   
Each blog in the corpus includes at least 200 occurrences of common English words. All formatting has been stripped with two exceptions. Individual posts within a single blogger are separated by the date of the following post and links within a post are denoted by the label urllink.
```

## 3. <a name="industry-names"></a>Getting all Industry Names
---

### 3.1. <a name="analyse-task"></a>Analysing the task

So our first step would be to make a set of all possible industries from the file name and store it in a variable that could be broadcast over the network.

As this is a really large dataset and we want to get a flavor of distributed processing, we use spark here to read all the files and use both the name and content for processing.

>It is always a good idea to write functions in python and test them on a sample data and then pass it as arguments to operation in spark.

So I always apply my functions first on sample data, and then pass it as arguments to RDD's to replicate them over the entire network.

### 3.2. <a name="read-dataset"/>Reading the dataset

I am using wholeTextFile to read an external dataset from a directory and save it in a RDD(resilient distributed dataset). As per [Spark Docs](https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html):

> SparkContext.wholeTextFiles() lets you read a directory containing multiple small text files, and returns each of them as (filename, content) pairs. 


Now that I have got all the blogs, I first use an operation on the filenames to extract all type of industries possible. As file name format is **user_id.gender.age.industry.star_sign.xml**, I use the 3rd word from the file name splitted from dot. 

```python
def get_industry_name(path):
    file_name = os.path.splitext(os.path.basename(path))
    return file_name[0].split('.')[-2]
```

I used this function and ran it on the file names obtained from sc.wholeTextFile(). I also persisted this RDD to store it in memory, to prevent recomputation in case of any failure.

```python
rddIndustrySet = data.map(lambda file: get_industry_name(file[0]))
rddIndustrySet.persist()
```

### 3.3. <a name="broadcast-industry"></a>Broadcasting set of Industries

Having the industry set, it is a good idea to broadcast this set over the network, as this would be needed later on when we are processing the blog posts for counting these industries. Since it is a read only shared variable over all remote executors, we are using broadcast variable to distribute it efficiently over the network. 

```python
industries = sc.broadcast(set(rddIndustrySet.collect()))
```
To access this industry set, I will simply have to take the value field of the broadcast variable industries, as it is a wrapper over the original data.

Following are the 40 industries found in the Blog Corpus:

>Accounting, Advertising, Agriculture, Architecture, Arts, Automotive, Banking, Biotech, BusinessServices, Chemicals, Communications-Media, Construction, Consulting, Education, Engineering, Environment, Fashion, Government, HumanResources, Internet, InvestmentBanking, Law, LawEnforcement-Security, Manufacturing, Maritime, Marketing, Military, Museums-Libraries, Non-Profit, Publishing, RealEstate, Religion, Science, Sports-Recreation, Student, Technology, Telecommunications, Tourism, Transportation, indUnk

Now that our industry set is shared across all executors, we have to use the content of each file to find mentions of industry and count them.

## 4. <a name="count-industry"></a>Counting Industry mentions over months
---

### 4.1. <a name="analyse-file"></a>Analysing the file content

Before beginning, let us look at the content of one sample file:

**17944.female.39.indUnk.Sagittarius.xml**

```
<Blog>

<date>18,May,2004</date>
<post>

	 
      this blog is not about
     
    
</post>


<date>18,June,2004</date>
<post>

	 
      this is a test to show my mother what a weblog is and how easy they are to start, but she doesnt give a
     
    
</post>
...
</Blog>
```
So all files have 3 types of tags,  

- \<Blog>,
- \<date>, and
- \<post> 

So, we have to find the industries within each \<post> tags and combine them with the date, to keep track how many times each industry is mentioned in this month.

### 4.2. <a name="clean-file"></a>Cleaning file content

I could have used an XML parser here, but the overhead of using such a parser outweighs a simple parsing task. So I used a simple parsing method of my own. To cleanup the files, I start with stripping off all the **\<Blog>, \</Blog>, \<date>, \<post>** tags and leading and trailing whitespace as they are unnecessary.

```python
content = file[1]
content = content.replace('<Blog>', '').replace('</Blog>', '')
content = content.replace('<date>', '').replace('<post>', '')
```

I also remove all extra spaces and newlines to get everything in one line.

```python
content = ' '.join(content.split()).strip()
```

My files would now look like this:

	18,May,2004</date>this blog is not about</post>18,June,2004</date> this is a test to show my mother what a weblog is and how easy they are to start, but she doesnt give a </post>

Splitting with the </post> tag splits the file into individual posts. 

```python
blog_list = content.split('</post>')[:-1]
```

I change them to a tuple of (MM-yyyy, post) tuple for easier processing, and then record the frequencies of each industry in each post.

```python
for line in blog_list:
    date, post = line.split('</date>')
    date = date.strip()
    post = post.strip()
    dmy = date.split(',')
    date = dmy[2]+'-'+dmy[1]
    pattern = re.compile('[^A-Za-z0-9 -]')
    post = pattern.sub('', post)
    blog_date_post_list.append((date, post))
```

Now, my post are in a tuple with (date, post) format.

### 4.3. <a name="count-month"></a>Counting Industry and the dates when they were mentioned

I now just have to count for each industry, how many time was it mentioned in a particular month, year. To do that, I create industry, MM-yyyy as the key, and the value as the number of time it was counted.

```python
counts = dict()
for date, post in blog_date_post_list:
    for word in post.split():
        word = word.lower()  # makes this case-insensitive
        for w in industries.value:
            if w.lower()==word:
                try:  # try/except KeyError is just a faster way to check if w is in counts:
                    counts[(w, date)] += 1
                except KeyError:
                    counts[(w, date)] = 1
print(sorted(list(counts.items())))
```

### 4.4. <a name="apply-spark"></a>Applying on Spark

Putting all this into a method that takes the content of the file and returns ((Industry, MM-yyyy), count)

```python
def parse_blog(file):
    content = file[1]
    content = content.replace('<Blog>', '').replace('</Blog>', '')
    content = content.replace('<date>', '').replace('<post>', '')
    content = ' '.join(content.split()).strip()
    
    blog_list = content.split('</post>')[:-1]
    blog_date_post_list = list()
    for line in blog_list:
        date, post = line.split('</date>')
        date = date.strip()
        post = post.strip()
        dmy = date.split(',')
        date = dmy[2]+'-'+dmy[1]
        pattern = re.compile('[^A-Za-z0-9 -]')
        post = pattern.sub('', post)
        blog_date_post_list.append((date, post))

    counts = dict()
    for date, post in blog_date_post_list:
        for word in post.split():
            word = word.lower()  # makes this case-insensitive
            for w in industries.value:
                if w.lower()==word:
                    try:  # try/except KeyError is just a faster way to check if w is in counts:
                        counts[(w, date)] += 1
                    except KeyError:
                        counts[(w, date)] = 1
    return sorted(list(counts.items()))
```

Using this on the Blog Corpus data RDD, I apply parse_blog on the content of the file, perform operation flatMap on the values returned, to get tuples such as:

> ((Industry, MM-yyyy), (count1, count2, count3, ...))

Now I just have to reduce it to get the count of each industry for one month.

```python
rddIndVsDate = data.flatMap(parse_blog).reduceByKey(lambda a, b: a+b)
```

Finally, I change the key to industry and (MM-yyyy, count) as value and group RDD's to get the final result: Industry and the number of times they have been mentioned in each month.

```python
rddFinal = rddIndVsDate.map(lambda value: (value[0][0], (value[0][1], value[1])))
rddFinal = rddFinal.sortBy(lambda x: x[1]).groupByKey().mapValues(list)
```

## 5. <a name="results"></a>Results
---

```python
rddFinal.saveAsTextFile('./output')
```
  
Arts:
>('Arts', [ ('1999-September', 1), ... ('2000-September', 1), ('2001-August', 1), ('2001-December', 1), ('2001-March', 1), ('2001-May', 1), ('2001-November', 1), ('2001-October', 1)]

Science:
>('Science', [('-', 1), ('1999-January', 1), ('2000-August', 2), ('2000-June', 1), ('2002-May', 14), ... ('2002-November', 43), ('2002-October', 38), ('2002-September', 24), ('2003-April', 91), ('2003-August', 130)] 

etc.

In the results, there are some months that are not english, or there is only -, because of some post containing non english dates, or blank dates. These require further cleaning and is left as future work.

Thanks for reading the post! You could look into the entire code in my github repo: [Blog Corpus Analysis](https://github.com/samanta-anupam/big_data_assignments/blob/master/BlogCorpus_industry_count.ipynb)

