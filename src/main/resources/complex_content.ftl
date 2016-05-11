<#macro header typenode>
<caption class="featureInfo">${typenode.name}</caption>
<tr>
    <#list typenode.attributes as attribute>
        <#if attribute.name != "FEATURE_LINK" && attribute.name != "address" && attribute.name != "design">
            <#if attribute.prefix == "">
                <th >${attribute.name}</th>
            <#else>
                <th >${attribute.prefix}:${attribute.name}</th>
            </#if>
        </#if>
    </#list>
</tr>
</#macro>


<table class="featureInfo">
<@header typenode=type />
    <tbody>
    <#list features as feature>
    <tr>
        <#list feature.attributes as attribute>
            <#if attribute.name != "FEATURE_LINK" && attribute.name != "address">
                <#if attribute.name != "design">
                    <td>${attribute.value?string}</td>
                </#if>
            </#if>
        </#list>
    </tr>
    </#list>
    </tbody>
</table>