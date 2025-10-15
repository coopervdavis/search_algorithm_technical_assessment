# Multi-Vehicle Search Take-Home Challenge

## Prompt

We'd like you to write a search algorithm that will allow renters to find locations where they could store multiple vehicles. Please write and deploy and endpoint that:

1. Accepts a request like:

   ```bash
   time curl -X POST "https://censored.amazonaws.com/default/neighborAssessmentLambda" \
       -H "Content-Type: application/json" \
       -d '[
               {
                   "length": 10,
                   "quantity": 2
               },
               {
                   "length": 20,
                   "quantity": 1
               },
               {
                   "length": 25,
                   "quantity": 1
               }
           ]'
   ```

   Each item in the request array represents vehicles that needs to be stored. The `length` is the length of the vehicle in feet. You can assume that the `width` of each vehicle is 10 feet. The `quantity` is how many vehicles of those dimensions need to be stored.

   The sum of all `quantity` values will always be less than or equal to 5.

1. Searches through the array of listings provided in the attached `listings.json` file. Where each listing looks like:

   ```json
   {
     "id": "abc123",
     "length": 10,
     "width": 20,
     "location_id": "def456",
     "price_in_cents": 100
   }
   ```

   All `length` and `width` values are multiples of 10.

1. And returns a response like:
   ```json
   [
     {
       "location_id": "abc123",
       "listing_ids": ["def456", "ghi789", "jkl012"],
       "total_price_in_cents": 300
     },
     {
       "location_id": "mno345",
       "listing_ids": ["pqr678", "stu901"],
       "total_price_in_cents": 305
     }
   ]
   ```
   The results should:
   1. Include every possible location that could store all requested vehicles
   1. Include the cheapest possible combination of listings per location
   1. Include only one result per location_id
   1. Be sorted by the total price in cents, ascending

## Assumptions

To simplify the problem, you should make two other assumptions:

1. Assume that, in each listing, vehicles will be stored at the same orientation
1. Assume that no buffer space is needed between vehicles

## Example

```bash
    curl -X POST "http://your-api.com" \
        -H "Content-Type: application/json" \
        -d '[
                {
                    "length": 10,
                    "quantity": 1
                }
            ]'
```

returns

```json
[
    {
        "location_id": "42b8f068-2d13-4ed1-8eec-c98f1eef0850",
        "listing_ids": ["b9bbe25f-5679-4917-bd7b-1e19c464f3a8"],
        "total_price_in_cents": 1005
    },
    {
        "location_id": "507628b8-163e-4e22-a6a3-6a16f8188928",
        "listing_ids": [ "e7d59481-b804-4565-b49b-d5beb7aec350" ],
        "total_price_in_cents": 1088
    },
    ... (362 more results) ...
    {
        "location_id":"22ad1ab7-d49b-49d6-8c30-531599934639",
        "listing_ids":["20cf6f5e-eb47-4104-b1f9-62527760a4c0"],
        "total_price_in_cents":99303
    }
]
```

## Submission

Please reply to this email with the following:

- Feedback on the project
- Duration
- Link to Github repo
- Link to API

We're primarily looking to see that candidates can get a solution working, deployed, and reasonably performant (<300ms, fine if there is a cold serverless startup). We advise you to spend no more than 6 total hours on this project.

Thank you and we look forward to reviewing your submission. We'll review every submission within 3 business days of receiving it and will notify you when you can take down your API.

## Hint

This problem is a variant of the [bin packing problem](https://en.wikipedia.org/wiki/Bin_packing_problem).
