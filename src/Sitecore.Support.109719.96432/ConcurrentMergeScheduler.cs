using Sitecore.ContentSearch.Diagnostics;
using System;

namespace Sitecore.Support
{
  public class ConcurrentMergeScheduler : Lucene.Net.Index.ConcurrentMergeScheduler
  {
    // Methods
    protected override void HandleMergeException(Exception exc)
    {
      try
      {
        base.HandleMergeException(exc);
      }
      catch (Exception exception)
      {
        CrawlingLog.Log.Fatal("SUPPORT LUCENE Merge operation has been finished with exception...", exception);
      }
    }
  }


}