const fs = require("fs");
const { parse } = require("csv-parse");

const dataDir = __dirname + "/data_files";

const advertisers = {};

console.info("Looking for advertisers");
fs.createReadStream("./Yashi_Advertisers.csv")
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    advertisers[row[0]] = row[1];
  })
  .on("end", function () {
    console.log("Found advertisers!");
    console.info("reading data files");
  })
  .on("error", function (error) {
    console.log(error.message);
  });

fs.readdir(dataDir, { withFileTypes: true }, (err, files) => {
  console.log("\nCurrent directory files:");
  if (err) console.log(err);
  else {
    files.forEach((file) => {
      const campaign = {};
      const campaign_data = [];
      let cmp_idx = 3;
      let cmp_incr = 1;
      let cmp_data_incr = 1;

      const order = {};
      const order_data = [];
      let ordr_idx = 5;
      let ordr_incr = 1;
      let ordr_data_incr = 1;

      const creative = {};
      const creative_data = [];
      let crt_idx = 7;
      let crt_incr = 1;
      let crt_data_incr = 1;
      fs.createReadStream("./data_files/" + file.name)
        .pipe(parse({ delimiter: ",", from_line: 2 }))
        .on("data", function (data) {
          // Check if it is in advertisers
          if (data[1] in advertisers) {
            const commonData = {
              log_date: data[0],
              impression_count: data[11],
              click_count: data[12],
              "25viewed_count": data[13],
              "50viewed_count": data[14],
              "75viewed_count": data[15],
              "100viewed_count": data[16],
            };

            //   Campaign operations
            if (!(data[cmp_idx] in campaign)) {
              campaign[data[cmp_idx]] = {
                campaign_id: cmp_incr,
                yashi_campaign_id: data[cmp_idx],
                name: data[cmp_idx + 1],
                yashi_advertiser_id: data[1],
                advertiser_name: data[2],
              };
              cmp_incr = cmp_incr + 1;
            }
            campaign_data.push({
              id: cmp_data_incr,
              campaign_id: data[cmp_idx],
              ...commonData,
            });
            cmp_data_incr = cmp_data_incr + 1;

            //   Order operations

            if (!(data[ordr_idx] in order)) {
              order[data[ordr_idx]] = {
                order_id: ordr_incr,
                yashi_order_id: data[ordr_idx],
                campaign_id: data[cmp_idx],
                name: data[ordr_idx + 1],
              };
              ordr_incr = ordr_incr + 1;
            }
            order_data.push({
              id: ordr_data_incr,
              order_id: data[ordr_idx],
              ...commonData,
            });
            ordr_data_incr = ordr_data_incr + 1;

            //   Creative operations

            if (!(data[crt_idx] in creative)) {
              creative[data[crt_idx]] = {
                creative_id: crt_incr,
                yashi_creative_id: data[crt_idx],
                order_id: data[ordr_idx],
                name: data[crt_idx + 1],
                preview_url: data[crt_idx + 2],
              };
              crt_incr = crt_incr + 1;
            }
            creative_data.push({
              id: crt_data_incr,
              creative_id: data[crt_idx],
              ...commonData,
            });
            crt_data_incr = crt_data_incr + 1;
          }
        })
        .on("end", function () {
          console.log(
            "finished for file: " + file.name,
            "campaign",
            Object.keys(campaign).length,
            "order",
            Object.keys(order).length,
            "creative",
            Object.keys(creative).length
          );
        })
        .on("error", function (error) {
          console.log(error.message);
        });
    });
  }
});
