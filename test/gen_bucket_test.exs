defmodule ProjectedBuckets.GenBucketTests  do
    use ExUnit.Case
    alias ProjectedBuckets.GenBucket

    test "bucket process starts with start_link" do
      assert {:ok, pid} = GenBucket.start_link
    end

    test "values can be put and retrieved" do
      {:ok, bucket} = GenBucket.start_link
      GenBucket.put(bucket, "foo", 42)
      assert GenBucket.get(bucket, "foo") == 42
    end
end
