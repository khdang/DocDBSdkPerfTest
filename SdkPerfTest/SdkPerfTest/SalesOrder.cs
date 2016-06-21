using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using System;

namespace SdkPerfTest
{
    public class SalesOrder
    {
        //You can use JsonProperty attributes to control how your objects are
        //handled by the Json Serializer/Deserializer
        //Any of the supported JSON.NET attributes here are supported, including the use of JsonConverters
        //if you really want fine grained control over the process

        //Here we are using JsonProperty to control how the Id property is passed over the wire
        //In this case, we're just making it a lowerCase string but you could entirely rename it
        //like we do with PurchaseOrderNumber below
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName="ponumber")]
        public string PurchaseOrderNumber { get; set; }
        public DateTime OrderDate { get; set; }
        public DateTime ShippedDate { get; set; }
        public string AccountNumber { get; set; }
        public decimal SubTotal { get; set; }
        public decimal TaxAmount { get; set; }
        public decimal Freight { get; set; }
        public decimal TotalDue { get; set; }
        public SalesOrderDetail[] Items { get; set; }

        public static SalesOrder GetSalesOrderSample(string documentId, string accountNumber)
        {
            return new SalesOrder
            {
                Id = documentId,
                AccountNumber = accountNumber,
                PurchaseOrderNumber = "PO18009186470",
                OrderDate = new DateTime(2005, 7, 1),
                SubTotal = 419.4589m,
                TaxAmount = 12.5838m,
                Freight = 472.3108m,
                TotalDue = 985.018m,
                Items = new SalesOrderDetail[]
                {
                    new SalesOrderDetail
                    {
                        OrderQty = 1,
                        ProductId = 760,
                        UnitPrice = 419.4589m,
                        LineTotal = 419.4589m
                    }
                },
            };
        }
    }
    public class SalesOrderDetail
    {
        public int OrderQty { get; set; }
        public int ProductId { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal LineTotal { get; set; }
    }
    public class SalesOrder2
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "ponumber")]
        public string PurchaseOrderNumber { get; set; }

        public DateTime OrderDate { get; set; }

        public DateTime DueDate { get; set; }

        public DateTime ShippedDate { get; set; }

        public string AccountNumber { get; set; }

        public decimal SubTotal { get; set; }

        public decimal TaxAmt { get; set; }

        public decimal Freight { get; set; }

        public decimal TotalDue { get; set; }

        public decimal DiscountAmt { get; set; }

        public SalesOrderDetail2[] Items { get; set; }
    }
    public class SalesOrderDetail2
    {
        public int OrderQty { get; set; }
        public string ProductCode { get; set; }
        public string ProductName { get; set; }
        public string CurrencySymbol { get; set; }
        public string CurrencyCode { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal LineTotal { get; set; }
    }
    internal class Product
    {
        string ProductCode { get; set; }
        string ProductName { get; set; }
        Price UnitPrice { get; set; }
    }
    internal class Price
    {
        double Amount { get; set; }
        string CurrencySymbol { get; set; }
        string CurrencyCode { get; set; }
    }

    /// <summary>
    /// SalesOrderDocument extends the Microsoft.Azure.Documents.Resource class
    /// This gives you access to internal properties of a Resource such as ETag, SelfLink, Id etc.
    /// When working with objects extending from Resource you get the benefit of not having to 
    /// dynamically cast between Document and your POCO.
    /// </summary>
    public class SalesOrderDocument : Resource
    {
        public string PurchaseOrderNumber { get; set; }
        public DateTime OrderDate { get; set; }
        public DateTime ShipDate { get; set; }
        public string AccountNumber { get; set; }
        public decimal SubTotal { get; set; }
        public decimal TaxAmt { get; set; }
        public decimal Freight { get; set; }
        public decimal TotalDue { get; set; }
        public SalesOrderDetail[] Items { get; set; }
    }
}
