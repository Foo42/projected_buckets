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
      IO.puts "reducer is alive? #{Process.alive? reducer}"
      assert reducer |> GenBucketReducer.value == 6
    end

    #How should reductions be done? as part of a bucket view, or completely seperate?
    # If it is done as part of bucket view it makes sense in one way, because then the resulting reduced view can be queried
    # What makes most sense in the planned use case. We will have a single view with a single reduction which produces a single value
    # In that case a wrapping reducer which turns
end
