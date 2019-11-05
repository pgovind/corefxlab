// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.ML;
using Xunit;

namespace Microsoft.Data.Tests
{
    public partial class DataFrameTests
    {
        [Fact]
        public void TestReadCsvWithHeader()
        {
            string data = @"vendor_id,rate_code,passenger_count,trip_time_in_secs,trip_distance,payment_type,fare_amount
CMT,1,1,1271,3.8,CRD,17.5
CMT,1,1,474,1.5,CRD,8
CMT,1,1,637,1.4,CRD,8.5
CMT,1,1,181,0.6,CSH,4.5";

            Stream GetStream(string streamData)
            {
                return new MemoryStream(Encoding.Default.GetBytes(streamData));
            }
            DataFrame df = DataFrame.LoadCsv(GetStream(data));
            Assert.Equal(4, df.RowCount);
            Assert.Equal(7, df.Columns.Count);
            Assert.Equal("CMT", df["vendor_id"][3]);

            DataFrame reducedRows = DataFrame.LoadCsv(GetStream(data), numberOfRowsToRead: 3);
            Assert.Equal(3, reducedRows.RowCount);
            Assert.Equal(7, reducedRows.Columns.Count);
            Assert.Equal("CMT", reducedRows["vendor_id"][2]);
        }

        [Fact]
        public void TestReadCsvNoHeader()
        {
            string data = @"CMT,1,1,1271,3.8,CRD,17.5
CMT,1,1,474,1.5,CRD,8
CMT,1,1,637,1.4,CRD,8.5
CMT,1,1,181,0.6,CSH,4.5";

            Stream GetStream(string streamData)
            {
                return new MemoryStream(Encoding.Default.GetBytes(streamData));
            }
            DataFrame df = DataFrame.LoadCsv(GetStream(data), header: false);
            Assert.Equal(4, df.RowCount);
            Assert.Equal(7, df.Columns.Count);
            Assert.Equal("CMT", df["Column0"][3]);

            DataFrame reducedRows = DataFrame.LoadCsv(GetStream(data), header: false, numberOfRowsToRead: 3);
            Assert.Equal(3, reducedRows.RowCount);
            Assert.Equal(7, reducedRows.Columns.Count);
            Assert.Equal("CMT", reducedRows["Column0"][2]);
        }

        private const string FOLDER = @"C:\Users\prgovi\Desktop\Work\machinelearning-samples\samples\csharp";

        private static string IssueModelPath = Path.Combine(FOLDER, @"issue-model.zip");

        private static string IssueFittedModelPath = Path.Combine(FOLDER, @"fitted-issue-model.zip");

        private static string DeployIssueFittedModelPath = Path.Combine(FOLDER, @"deploy-fitted-issue-model.zip");

        private static string LabelColumnName = "Area";

        private static void Train(MLContext mlContext, IDataView trainData, IDataView validateData, IDataView testData, string modelpath, string fittedModelPath, string productionModelPath)

        {
            var experimentSettings = new MulticlassExperimentSettings();
            experimentSettings.MaxExperimentTimeInSeconds = 60;
            var cts = new System.Threading.CancellationTokenSource();
            experimentSettings.CancellationToken = cts.Token;
            ExperimentResult<ML.Data.MulticlassClassificationMetrics> experimentResult = mlContext.Auto().CreateMulticlassClassificationExperiment(experimentSettings).
                Execute(trainData, LabelColumnName, progressHandler: null);
            RunDetail<ML.Data.MulticlassClassificationMetrics> bestRun = experimentResult.BestRun;

            ITransformer trainedModel = bestRun.Model;

            EvaluateModelAndPrintMetrics(mlContext, trainedModel, bestRun.TrainerName, validateData);
        }

        [Fact]
        public void Demo()
        {
            string filename = Path.Combine(FOLDER, "GitHubIssueDownloaderFormat.tsv");
            DataFrame df = DataFrame.LoadCsv(filename, separator: '\t', header: true);
            // Goal is to train an IssueLabeller
            // Step1: Our GitHub data combines issues and PRs. I segment into two different DataFrames, one for PR, one for Issues
            DataFrame prs = df.Filter(df["IsPR"].ElementwiseEquals(1));
            DataFrame issues = df.Filter(df["IsPR"].ElementwiseEquals(0));

            // For our model training, the "ID" and "FilePaths" do not always correlate well with our label "Area". How do I know? I found the Pearson correlation coefficient between the columns like so:
            var corr = LinqStatistics.EnumerableStats.Pearson(df["FilePaths"].Cast<string>(), df["Area"].Cast<float>());

            issues.Columns.Remove("ID");
            issues.Columns.Remove("FilePaths");

            // However, most issues/PRs mention users/owners. So, parse Descriptions, find user mentions and add it as a new column to issues
            StringDataFrameColumn description = issues["Description"] as StringDataFrameColumn;
            StringDataFrameColumn userMentionsColumn = new StringDataFrameColumn("UserMentions", issues.RowCount);
            var regex = new Regex(@"@[a-zA-Z0-9_//-]+");
            description.ApplyElementwise((string current, long rowIndex) =>
            {
                var userMentions = regex.Matches(current).Select(x => x.Value).ToArray();
                userMentionsColumn[rowIndex] = string.Join(' ', userMentions);
                return current;
            });
            issues.Columns.Add(userMentionsColumn);

            // Step2: The input to training is three files for each of the two: Train dataset (80%), Validate dataset (next 10%), test dataset (last 10%)
            DataFrame trainIssues = SplitTrainTest(issues, 0.8f, out DataFrame tempValidateAndTest);
            DataFrame validateIssues = SplitTrainTest(tempValidateAndTest, 0.5f, out DataFrame testIssues);

            MLContext mLContext = new MLContext();

            Train(mLContext, trainIssues, validateIssues, testIssues, IssueModelPath, IssueFittedModelPath, DeployIssueFittedModelPath);
        }

        [Fact]
        private static DataFrame CalculateNumberOfTrips(
            DataFrame inputDataFrame,
            string dateField)
        {
            /*
             * Data looks like the following
             * city_id, date, active_vehicles, trips
                02512, 1/1/2015, 190, 1132
                02765, 1/1/2015, 225, 1765
                02764, 1/1/2015, 3427, 29421
                02682, 1/1/2015, 945, 7679
                02617, 1/1/2015, 1228, 9537
                02598, 1/1/2015, 870, 6903
                02598, 1/2/2015, 785, 4768
                02617, 1/2/2015, 1137, 7065
                02512, 1/2/2015, 175, 875
                02682, 1/2/2015, 890, 5506
                02765, 1/2/2015, 196, 1001
                02764, 1/2/2015, 3147, 19974
                02765, 1/3/2015, 201, 1526 
                02512, 1/3/2015, 173, 1088
                .
                .
                .
             */
            int GetDayNumber(string date)
            {
                return (int)Convert.ToDateTime(date).DayOfWeek;
            }

            PrimitiveDataFrameColumn<int> dayNumberColumn = new PrimitiveDataFrameColumn<int>("DayNumber", inputDataFrame.RowCount);
            for (long i = 0; i < inputDataFrame.RowCount; i++)
            {
                dayNumberColumn[i] = GetDayNumber((string)inputDataFrame[dateField][i] ?? "-1/-1/-1");
            }
            inputDataFrame["DayNumber"] = dayNumberColumn;

            // Doesn't make sense to Sum() the "date" column
            inputDataFrame.Columns.Remove("date");

            // GroupBy the DayNumber and Sum
            DataFrame groupedSum = inputDataFrame.GroupBy("DayNumber").Sum();

            DataFrame ret = new DataFrame();
            ret["day_of_the_week"] = groupedSum["DayNumber"];
            ret["active_vehicles"] = groupedSum["active_vehicles"];
            ret["number_of_trips"] = groupedSum["trips"];

            return ret;
        }
    }
}
