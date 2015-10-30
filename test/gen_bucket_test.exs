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

    test "stream_changes returns stream of bucket changes" do
      test_pid = self()
      {:ok, bucket} = GenBucket.start_link
      spawn_link fn ->
        bucket |> GenBucket.stream_changes |> Stream.each(fn change -> send(test_pid, change) end) |> Stream.run
      end

      :timer.sleep(50) #Got to be a better way to know that the stream iterator is in place
      GenBucket.put(bucket, "foo", 42)
      assert_receive {:put, {"foo", 42}}
    end

    test "can install a view with a mapping function and query mapped values using view name" do
      {:ok, bucket} = GenBucket.start_link
      GenBucket.put(bucket, "old", 1)
      double_values = fn {key,value} -> {key,value * 2} end
      GenBucket.install_view(bucket, "doubled", double_values)
      :timer.sleep(50)
      GenBucket.put(bucket, "foo", 42)
      :timer.sleep(50)
      assert GenBucket.get(bucket, "doubled", "foo") == 84, "values inserted after view should be mapped"
      assert GenBucket.get(bucket, "doubled", "old") == 2, "original values should be mapped" #Tricky to do without there being a gap between getting full content and starting update stream
    end
end
