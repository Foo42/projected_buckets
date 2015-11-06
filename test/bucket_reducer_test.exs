defmodule ProjectedBuckets.BucketReducerTests  do
    use ExUnit.Case
    alias ProjectedBuckets.GenBucket
    alias ProjectedBuckets.GenBucketReducer

    test "reducer will follow a bucket it is passed" do
      {:ok, bucket} = GenBucket.start_link
      GenBucket.put bucket, "a", 2
      {:ok, reducer} = GenBucketReducer.start_link 0, fn {_k,v}, acc -> acc + v end
      reducer |> GenBucketReducer.follow bucket

      :timer.sleep(50)
      GenBucket.put bucket, "b", 4

      :timer.sleep(50)
      assert reducer |> GenBucketReducer.value == 6
    end
end
